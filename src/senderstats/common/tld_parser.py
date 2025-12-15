from __future__ import annotations

import pickle
from functools import lru_cache
from importlib import resources
from typing import Any, Dict, Iterable, List, Tuple

try:
    import requests
except Exception:
    requests = None

_PSL_URL = "https://publicsuffix.org/list/public_suffix_list.dat"
_RESOURCE_PACKAGE = "senderstats.common.data"
_RESOURCE_PSL = "default_psl.pkl"

class TLDParser:
    """
    Public Suffix List (PSL) trie manager + splitter.

    Nodes are stored as plain built-in objects so pickles are stable across refactors:
      node = {"c": {label: child_index}, "r": bool, "w": bool, "e": bool}
        c = children
        r = is_rule
        w = is_wildcard
        e = is_exception
    """
    __slots__ = ("nodes",)

    def __init__(self, nodes: List[Dict[str, Any]]):
        self.nodes = nodes

    @classmethod
    def load(cls, path: str) -> TLDParser:
        with open(path, "rb") as f:
            nodes = pickle.load(f)

        if not isinstance(nodes, list):
            raise TypeError(f"Invalid trie pickle: expected list, got {type(nodes)!r}")

        # Optional but recommended: cheap schema sanity check
        if nodes:
            node = nodes[0]
            if not isinstance(node, dict) or "c" not in node:
                raise TypeError("Invalid trie pickle: bad node schema")

        return cls(nodes)

    @classmethod
    def load_default(cls) -> TLDParser:
        with resources.files(_RESOURCE_PACKAGE).joinpath(_RESOURCE_PSL).open("rb") as f:
            nodes = pickle.load(f)
        cls._validate_nodes(nodes)
        return cls(nodes)

    @staticmethod
    def _validate_nodes(nodes: Any) -> None:
        if not isinstance(nodes, list):
            raise TypeError(f"Invalid trie pickle: expected list, got {type(nodes)!r}")
        if nodes:
            node0 = nodes[0]
            if not isinstance(node0, dict) or "c" not in node0:
                raise TypeError("Invalid trie pickle: bad node schema")

    def save(self, path: str) -> None:
        # Only built-ins => stable pickle
        with open(path, "wb") as f:
            pickle.dump(self.nodes, f, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def fetch_psl_text(
            url: str = _PSL_URL,
            timeout: float = 20.0,
            user_agent: str = "tld-trie/1.0",
    ) -> str:
        if requests is None:
            raise RuntimeError(
                "requests is not installed. Install it or provide PSL text directly."
            )
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": user_agent})
        resp.raise_for_status()
        return resp.text

    @classmethod
    def from_psl_text(cls, psl_text: str) -> TLDParser:
        return cls(cls.build_trie(psl_text))

    @classmethod
    def update_from_internet(
            cls,
            pickle_path: str,
            url: str = _PSL_URL,
            timeout: float = 30.0,
    ) -> TLDParser:
        text = cls.fetch_psl_text(url=url, timeout=timeout)
        inst = cls.from_psl_text(text)
        inst.save(pickle_path)
        return inst

    @staticmethod
    def parse_psl_lines(psl_text: str) -> Iterable[str]:
        for raw in psl_text.splitlines():
            line = raw.strip()
            if not line or line.startswith("//"):
                continue
            yield line.split()[0]

    @staticmethod
    def build_trie(psl_text: str) -> List[Dict[str, Any]]:
        # root node
        nodes: List[Dict[str, Any]] = [{"c": {}, "r": False, "w": False, "e": False}]

        def get_child(parent_idx: int, label: str) -> int:
            parent = nodes[parent_idx]
            children: Dict[str, int] = parent["c"]
            nxt = children.get(label)
            if nxt is None:
                nxt = len(nodes)
                children[label] = nxt
                nodes.append({"c": {}, "r": False, "w": False, "e": False})
            return nxt

        for rule in TLDParser.parse_psl_lines(psl_text):
            is_exc = rule.startswith("!")
            if is_exc:
                rule = rule[1:]

            labels = rule.lower().split(".")[::-1]

            cur = 0
            for lab in labels:
                cur = get_child(cur, lab)

            if is_exc:
                nodes[cur]["e"] = True
            else:
                nodes[cur]["r"] = True

            if labels and labels[0] == "*":
                nodes[cur]["w"] = True

        return nodes

    # -------------------------
    # Splitting
    # -------------------------

    def split_host(self, host: str) -> Tuple[str, str, str]:
        """
        Returns (subdomain, registrable_domain, public_suffix)

        Notes:
        - Assumes `host` is a hostname (not a URL). Caller should strip scheme/port.
        - For IPs / single-label names, returns ("", host, "").
        """
        h = host.strip().rstrip(".").lower()
        if not h or "." not in h:
            return ("", h, "")

        labels = h.split(".")
        n = len(labels)

        best_len = 1  # PSL implicit "*": suffix is last label
        cur = 0
        matched_depth = 0
        nodes = self.nodes  # local for speed

        for i in range(n - 1, -1, -1):
            lab = labels[i]
            node = nodes[cur]
            children = node["c"]

            nxt = children.get(lab)
            if nxt is None:
                # try wildcard at this level
                w = children.get("*")
                if w is not None:
                    best_len = max(best_len, matched_depth + 1)
                break

            cur = nxt
            matched_depth += 1
            node = nodes[cur]

            if node["e"]:
                best_len = max(best_len, matched_depth - 1)
                break

            if node["r"]:
                best_len = max(best_len, matched_depth)

            if node["c"].get("*") is not None:
                best_len = max(best_len, matched_depth + 1)

        if best_len >= n:
            return ("", h, h)

        public_suffix = ".".join(labels[-best_len:])
        registrable = ".".join(labels[-(best_len + 1):])
        sub = ".".join(labels[:-(best_len + 1)])
        return (sub, registrable, public_suffix)

    def split_host_extended(self, host: str) -> Tuple[str, str, str, str]:
        """
        Returns (host_label, subdomain, registrable_domain, public_suffix)
        """
        sub, registrable, suffix = self.split_host(host)
        if not sub:
            return ("", "", registrable, suffix)

        host_label, sep, rest = sub.partition(".")
        subdomain = rest if sep else ""
        return (host_label, subdomain, registrable, suffix)

    def split_host_extended_parallel(
            self, hosts: Iterable[str]
    ) -> Tuple[List[str], List[str], List[str], List[str]]:
        """
        Batch split for speed. Returns 4 parallel lists:
          host_label[], subdomain[], registrable[], suffix[]
        """
        host_labels: List[str] = []
        subdomains: List[str] = []
        registrables: List[str] = []
        suffixes: List[str] = []

        for h in hosts:
            hl, sd, reg, suf = self.split_host_extended(h)
            host_labels.append(hl)
            subdomains.append(sd)
            registrables.append(reg)
            suffixes.append(suf)

        return host_labels, subdomains, registrables, suffixes


@lru_cache(maxsize=1)
def get_default_tld_parser() -> TLDParser:
    return TLDParser.load_default()


def split_host(host: str) -> tuple[str, str, str]:
    return get_default_tld_parser().split_host(host)


def split_host_extended(host: str) -> tuple[str, str, str, str]:
    return get_default_tld_parser().split_host_extended(host)
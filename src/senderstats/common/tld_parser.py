from __future__ import annotations

import pickle
from functools import lru_cache
from importlib import resources
from typing import Any, Dict, Iterable, List, Tuple

_RESOURCE_PACKAGE = "senderstats.common.data"
_RESOURCE_PSL = "default_psl.pkl"

# On-disk schema (pickle artifact):
RawNode = Dict[str, Any]

# Fast runtime schema
Node = Tuple[Dict[str, int], bool, bool, bool]


class TLDParser:
    """
    PSL trie splitter.

    Loads a stable dict-based trie pickle from package resources and converts once into
    a compact tuple-based runtime representation optimized for splitting.
    """

    __slots__ = ("nodes",)

    def __init__(self, raw_nodes: List[RawNode]):
        # Convert stable pickle schema -> faster runtime schema
        self.nodes: List[Node] = [
            (n["c"], bool(n["r"]), ("*" in n["c"]), bool(n["e"]))
            for n in raw_nodes
        ]

    @classmethod
    def load_default(cls) -> TLDParser:
        with resources.files(_RESOURCE_PACKAGE).joinpath(_RESOURCE_PSL).open("rb") as f:
            raw_nodes = pickle.load(f)
        cls._validate_raw_nodes(raw_nodes)
        return cls(raw_nodes)

    @staticmethod
    def _validate_raw_nodes(raw_nodes: Any) -> None:
        if not isinstance(raw_nodes, list):
            raise TypeError(f"Invalid trie pickle: expected list, got {type(raw_nodes)!r}")
        if raw_nodes:
            n0 = raw_nodes[0]
            if not isinstance(n0, dict) or "c" not in n0:
                raise TypeError("Invalid trie pickle: bad node schema")

    # -------------------------
    # Splitting, yes it's redundant but to prevent frames
    # -------------------------

    def split_host_unchecked(self, h: str) -> Tuple[str, str, str]:
        # expects: lowercase hostname, no scheme/port, ideally trimmed/no trailing dot
        if __debug__:
            assert isinstance(h, str)
            assert h == h.strip()
            assert not h.endswith(".")
            assert h == h.lower()

        if not h or "." not in h:
            return ("", h, "")

        labels = h.split(".")
        n = len(labels)

        best_len = 1
        cur = 0
        matched_depth = 0
        nodes = self.nodes

        for i in range(n - 1, -1, -1):
            children = nodes[cur][0]
            lab = labels[i]

            nxt = children.get(lab)
            if nxt is None:
                if children.get("*") is not None:
                    cand = matched_depth + 1
                    if cand > best_len:
                        best_len = cand
                break

            cur = nxt
            matched_depth += 1

            children, is_rule, has_star_child, is_exc = nodes[cur]

            if is_exc:
                cand = matched_depth - 1
                if cand > best_len:
                    best_len = cand
                break

            if is_rule and matched_depth > best_len:
                best_len = matched_depth

            if has_star_child:
                cand = matched_depth + 1
                if cand > best_len:
                    best_len = cand

        if best_len >= n:
            return ("", h, h)

        public_suffix = ".".join(labels[-best_len:])
        registrable = ".".join(labels[-(best_len + 1):])
        sub = ".".join(labels[:-(best_len + 1)])
        return (sub, registrable, public_suffix)

    def split_host_safe(self, host: str) -> Tuple[str, str, str]:
        # Safe: trim, strip trailing dots, lowercase
        h = (host or "").strip().rstrip(".").lower()

        # expects: lowercase hostname, no scheme/port, ideally trimmed/no trailing dot
        if not h or "." not in h:
            return ("", h, "")

        labels = h.split(".")
        n = len(labels)

        best_len = 1
        cur = 0
        matched_depth = 0
        nodes = self.nodes

        for i in range(n - 1, -1, -1):
            children = nodes[cur][0]
            lab = labels[i]

            nxt = children.get(lab)
            if nxt is None:
                if children.get("*") is not None:
                    cand = matched_depth + 1
                    if cand > best_len:
                        best_len = cand
                break

            cur = nxt
            matched_depth += 1

            children, is_rule, has_star_child, is_exc = nodes[cur]

            if is_exc:
                cand = matched_depth - 1
                if cand > best_len:
                    best_len = cand
                break

            if is_rule and matched_depth > best_len:
                best_len = matched_depth

            if has_star_child:
                cand = matched_depth + 1
                if cand > best_len:
                    best_len = cand

        if best_len >= n:
            return ("", h, h)

        public_suffix = ".".join(labels[-best_len:])
        registrable = ".".join(labels[-(best_len + 1):])
        sub = ".".join(labels[:-(best_len + 1)])
        return (sub, registrable, public_suffix)

    def split_host_batch_unchecked(self, hosts: Iterable[str]) -> List[Tuple[str, str, str]]:
        out: List[Tuple[str, str, str]] = []
        ap = out.append
        nodes = self.nodes  # bind once

        for h in hosts:
            # expects: lowercase hostname, no scheme/port, ideally trimmed/no trailing dot
            if __debug__:
                assert isinstance(h, str)
                assert h == h.strip()
                assert not h.endswith(".")
                assert h == h.lower()

            if not h or "." not in h:
                ap(("", h, ""))
                continue

            labels = h.split(".")
            n = len(labels)

            best_len = 1
            cur = 0
            matched_depth = 0

            for i in range(n - 1, -1, -1):
                children = nodes[cur][0]
                lab = labels[i]

                nxt = children.get(lab)
                if nxt is None:
                    if children.get("*") is not None:
                        cand = matched_depth + 1
                        if cand > best_len:
                            best_len = cand
                    break

                cur = nxt
                matched_depth += 1

                children, is_rule, has_star_child, is_exc = nodes[cur]

                if is_exc:
                    cand = matched_depth - 1
                    if cand > best_len:
                        best_len = cand
                    break

                if is_rule and matched_depth > best_len:
                    best_len = matched_depth

                if has_star_child:
                    cand = matched_depth + 1
                    if cand > best_len:
                        best_len = cand

            if best_len >= n:
                ap(("", h, h))
                continue

            public_suffix = ".".join(labels[-best_len:])
            registrable = ".".join(labels[-(best_len + 1):])
            sub = ".".join(labels[:-(best_len + 1)])
            ap((sub, registrable, public_suffix))

        return out

    def split_host_batch_safe(self, hosts: Iterable[str]) -> List[Tuple[str, str, str]]:
        out: List[Tuple[str, str, str]] = []
        ap = out.append
        nodes = self.nodes  # bind once

        for host in hosts:
            h = (host or "").strip().rstrip(".").lower()

            # expects: lowercase hostname, no scheme/port, ideally trimmed/no trailing dot
            if not h or "." not in h:
                ap(("", h, ""))
                continue

            labels = h.split(".")
            n = len(labels)

            best_len = 1
            cur = 0
            matched_depth = 0

            for i in range(n - 1, -1, -1):
                children = nodes[cur][0]
                lab = labels[i]

                nxt = children.get(lab)
                if nxt is None:
                    if children.get("*") is not None:
                        cand = matched_depth + 1
                        if cand > best_len:
                            best_len = cand
                    break

                cur = nxt
                matched_depth += 1

                children, is_rule, has_star_child, is_exc = nodes[cur]

                if is_exc:
                    cand = matched_depth - 1
                    if cand > best_len:
                        best_len = cand
                    break

                if is_rule and matched_depth > best_len:
                    best_len = matched_depth

                if has_star_child:
                    cand = matched_depth + 1
                    if cand > best_len:
                        best_len = cand

            if best_len >= n:
                ap(("", h, h))
                continue

            public_suffix = ".".join(labels[-best_len:])
            registrable = ".".join(labels[-(best_len + 1):])
            sub = ".".join(labels[:-(best_len + 1)])
            ap((sub, registrable, public_suffix))

        return out

    def split_host_extended_batch_unchecked(
            self, hosts: Iterable[str]
    ) -> Tuple[List[str], List[str], List[str], List[str]]:
        host_labels: List[str] = []
        subdomains: List[str] = []
        registrables: List[str] = []
        suffixes: List[str] = []

        hl_append = host_labels.append
        sd_append = subdomains.append
        reg_append = registrables.append
        suf_append = suffixes.append

        nodes = self.nodes  # bind once

        for h in hosts:
            # expects: lowercase hostname, no scheme/port, ideally trimmed/no trailing dot
            if __debug__:
                assert isinstance(h, str)
                assert h == h.strip()
                assert not h.endswith(".")
                assert h == h.lower()

            if not h or "." not in h:
                # core result would be ("", h, "")
                # extended mapping: if not sub => ("", "", registrable, suffix)
                hl_append("")
                sd_append("")
                reg_append(h)
                suf_append("")
                continue

            labels = h.split(".")
            n = len(labels)

            best_len = 1
            cur = 0
            matched_depth = 0

            for i in range(n - 1, -1, -1):
                children = nodes[cur][0]
                lab = labels[i]

                nxt = children.get(lab)
                if nxt is None:
                    if children.get("*") is not None:
                        cand = matched_depth + 1
                        if cand > best_len:
                            best_len = cand
                    break

                cur = nxt
                matched_depth += 1

                children, is_rule, has_star_child, is_exc = nodes[cur]

                if is_exc:
                    cand = matched_depth - 1
                    if cand > best_len:
                        best_len = cand
                    break

                if is_rule and matched_depth > best_len:
                    best_len = matched_depth

                if has_star_child:
                    cand = matched_depth + 1
                    if cand > best_len:
                        best_len = cand

            if best_len >= n:
                # core: ("", h, h) -> extended: ("", "", registrable, suffix)
                hl_append("")
                sd_append("")
                reg_append(h)
                suf_append(h)
                continue

            suffix = ".".join(labels[-best_len:])
            registrable = ".".join(labels[-(best_len + 1):])
            sub = ".".join(labels[:-(best_len + 1)])

            if not sub:
                hl_append("")
                sd_append("")
                reg_append(registrable)
                suf_append(suffix)
            else:
                host_label, sep, rest = sub.partition(".")
                hl_append(host_label)
                sd_append(rest if sep else "")
                reg_append(registrable)
                suf_append(suffix)

        return host_labels, subdomains, registrables, suffixes

    def split_host_extended_batch_safe(
            self, hosts: Iterable[str]
    ) -> Tuple[List[str], List[str], List[str], List[str]]:
        host_labels: List[str] = []
        subdomains: List[str] = []
        registrables: List[str] = []
        suffixes: List[str] = []

        hl_append = host_labels.append
        sd_append = subdomains.append
        reg_append = registrables.append
        suf_append = suffixes.append

        nodes = self.nodes  # bind once

        for host in hosts:
            h = (host or "").strip().rstrip(".").lower()

            # normalized string might be empty / no dot
            if not h or "." not in h:
                hl_append("")
                sd_append("")
                reg_append(h)
                suf_append("")
                continue

            labels = h.split(".")
            n = len(labels)

            best_len = 1
            cur = 0
            matched_depth = 0

            for i in range(n - 1, -1, -1):
                children = nodes[cur][0]
                lab = labels[i]

                nxt = children.get(lab)
                if nxt is None:
                    if children.get("*") is not None:
                        cand = matched_depth + 1
                        if cand > best_len:
                            best_len = cand
                    break

                cur = nxt
                matched_depth += 1

                children, is_rule, has_star_child, is_exc = nodes[cur]

                if is_exc:
                    cand = matched_depth - 1
                    if cand > best_len:
                        best_len = cand
                    break

                if is_rule and matched_depth > best_len:
                    best_len = matched_depth

                if has_star_child:
                    cand = matched_depth + 1
                    if cand > best_len:
                        best_len = cand

            if best_len >= n:
                hl_append("")
                sd_append("")
                reg_append(h)
                suf_append(h)
                continue

            suffix = ".".join(labels[-best_len:])
            registrable = ".".join(labels[-(best_len + 1):])
            sub = ".".join(labels[:-(best_len + 1)])

            if not sub:
                hl_append("")
                sd_append("")
                reg_append(registrable)
                suf_append(suffix)
            else:
                host_label, sep, rest = sub.partition(".")
                hl_append(host_label)
                sd_append(rest if sep else "")
                reg_append(registrable)
                suf_append(suffix)

        return host_labels, subdomains, registrables, suffixes


@lru_cache(maxsize=1)
def get_default_tld_parser() -> TLDParser:
    return TLDParser.load_default()

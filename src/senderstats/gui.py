import io
import json
import os
import queue
import sys
import threading
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
from importlib import resources
from types import SimpleNamespace

import regex as re

try:
    import tkinter as tk
    from tkinter import filedialog, ttk, messagebox, scrolledtext
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError as e:
    raise SystemExit(
        "GUI mode requires Tkinter and tkinterdnd2.\n\n"
        "Install GUI dependencies with:\n"
        "  pip install senderstats[gui]\n\n"
        "And install system Tk support (Linux examples):\n"
        "  Debian/Ubuntu: sudo apt install python3-tk\n"
        "  Fedora:        sudo dnf install python3-tkinter\n"
        "  Arch:          sudo pacman -S tk\n"
    ) from e

from senderstats.cli_args import get_version
from senderstats.common.defaults import *
from senderstats.common.regex_patterns import EMAIL_ADDRESS_REGEX, VALID_DOMAIN_REGEX, IPV46_REGEX
from senderstats.data.data_source_type import DataSourceType
from senderstats.processing.config_manager import ConfigManager
from senderstats.processing.data_source_manager import DataSourceManager
from senderstats.processing.pipeline_manager import PipelineManager
from senderstats.processing.pipeline_processor import PipelineProcessor
from senderstats.reporting.pipeline_processor_report import PipelineProcessorReport


def is_valid_domain_syntax(domain_name: str):
    if not re.match(VALID_DOMAIN_REGEX, domain_name, re.IGNORECASE):
        raise ValueError(f"Invalid domain name syntax: {domain_name}")
    return domain_name


def is_valid_ip_syntax(ip: str):
    if not re.match(IPV46_REGEX, ip, re.IGNORECASE):
        raise ValueError(f"Invalid ip address syntax: {ip}")
    return ip


def is_valid_email_syntax(email: str):
    if not re.match(EMAIL_ADDRESS_REGEX, email, re.IGNORECASE):
        raise ValueError(f"Invalid email address syntax: {email}")
    return email


def validate_xlsx_file(file_path):
    if not file_path.lower().endswith('.xlsx'):
        raise ValueError("File must have a .xlsx extension.")
    return file_path


def set_app_icon(root: tk.Tk):
    try:
        with resources.path("senderstats.images", "senderstats.png") as p:
            icon = tk.PhotoImage(file=str(p))
            root.iconphoto(True, icon)
            root._icon_ref = icon
    except Exception:
        pass

    # Windows-specific ICO
    if sys.platform.startswith("win"):
        try:
            with resources.path("senderstats.images", "senderstats.ico") as p:
                root.iconbitmap(str(p))
        except Exception:
            pass


class QueueOutput(io.TextIOBase):
    def __init__(self, q):
        self.q = q

    def write(self, text):
        if text:
            self.q.put(("output", text))
        return len(text)

    def flush(self):
        pass


class SenderStatsGUI:
    def __init__(self, root):
        set_app_icon(root)
        self.root = root
        self.root.title(f"SenderStats v{get_version()}")
        self.root.geometry("1024x800")
        self.root.minsize(1024, 800)
        self.root.columnconfigure(0, weight=1)
        home_dir = os.path.expanduser("~")
        self.config_dir = os.path.join(home_dir, ".senderstats")
        self.config_file = os.path.join(self.config_dir, "settings.json")
        os.makedirs(self.config_dir, exist_ok=True)
        # Give the main areas reasonable proportions:
        #  - row 0: notebook (tabs)
        #  - row 1: log/status
        #  - row 2: run button
        self.root.rowconfigure(0, weight=3)  # notebook
        self.root.rowconfigure(1, weight=2)  # log
        self.root.rowconfigure(2, weight=0)  # run button (natural size)

        # TK variables with defaults
        self.input_files = []
        self.output_file = tk.StringVar()
        self.ip_field = tk.StringVar(value=DEFAULT_IP_FIELD)
        self.mfrom_field = tk.StringVar(value=DEFAULT_MFROM_FIELD)
        self.hfrom_field = tk.StringVar(value=DEFAULT_HFROM_FIELD)
        self.rcpts_field = tk.StringVar(value=DEFAULT_RCPTS_FIELD)
        self.rpath_field = tk.StringVar(value=DEFAULT_RPATH_FIELD)
        self.msgid_field = tk.StringVar(value=DEFAULT_MSGID_FIELD)
        self.subject_field = tk.StringVar(value=DEFAULT_SUBJECT_FIELD)
        self.msgsz_field = tk.StringVar(value=DEFAULT_MSGSZ_FIELD)
        self.date_field = tk.StringVar(value=DEFAULT_DATE_FIELD)
        self.gen_hfrom = tk.BooleanVar()
        self.gen_rpath = tk.BooleanVar()
        self.gen_alignment = tk.BooleanVar()
        self.gen_msgid = tk.BooleanVar()
        self.expand_recipients = tk.BooleanVar()
        self.no_display = tk.BooleanVar(value=True)
        self.remove_prvs = tk.BooleanVar(value=True)
        self.decode_srs = tk.BooleanVar(value=True)
        self.normalize_bounces = tk.BooleanVar(value=True)
        self.normalize_entropy = tk.BooleanVar()
        self.no_empty_hfrom = tk.BooleanVar()
        self.sample_subject = tk.BooleanVar(value=True)
        self.exclude_ips = []
        self.exclude_domains = []
        self.restrict_domains = []
        self.exclude_senders = []
        self.exclude_dup_msgids = tk.BooleanVar()
        self.date_format = tk.StringVar(value=DEFAULT_DATE_FORMAT)
        self.no_default_exclude_domains = tk.BooleanVar()
        self.no_default_exclude_ips = tk.BooleanVar()

        # Output file was set manually
        self.output_file_set_manually = False

        # Notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.grid(row=0, column=0, sticky='nsew')

        self.create_input_output_tab()
        self.create_field_mapping_tab()
        self.create_reporting_tab()
        self.create_parsing_tab()
        self.create_filters_tab()

        # Log / Status frame
        log_frame = ttk.LabelFrame(root, text="Log / Status", height=8)
        log_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")

        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.output_text = scrolledtext.ScrolledText(log_frame, height=8, state='disabled')
        self.output_text.grid(row=0, column=0, padx=(10, 5), pady=10, sticky='nsew')

        # Clear Log button
        self.clear_log_button = ttk.Button(log_frame, text="Clear Log", command=self.clear_log)
        self.clear_log_button.grid(row=1, column=0, pady=(0, 5), padx=(0, 10), sticky='e')

        # Run button
        self.run_button = ttk.Button(root, text="Run SenderStats", command=self.run_tool)
        self.run_button.grid(row=2, column=0, sticky='ew', padx=10, pady=(0, 10))

        # Set initial focus to input listbox once everything is created
        self.root.after(0, lambda: self.input_listbox.focus_set())
        self.result_queue = queue.Queue()
        self.__start_queue_watcher()
        self.load_settings()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def __start_queue_watcher(self):
        def watcher():
            while True:
                msg, arg = self.result_queue.get()  # blocks without freezing GUI
                # Always handle messages in the Tkinter thread
                self.root.after(0, self.__handle_queue_message, msg, arg)

        t = threading.Thread(target=watcher, daemon=True)
        t.start()

    def __handle_queue_message(self, msg, arg):
        if msg == "output":
            self.output_text.config(state='normal')
            self.output_text.insert(tk.END, arg)
            self.output_text.config(state='disabled')
            self.output_text.see(tk.END)

        elif msg == "success":
            self.run_button.config(state='normal', text="Run SenderStats")

        elif msg == "error":
            messagebox.showerror("Unexpected Error", arg)
            self.run_button.config(state='normal', text="Run SenderStats")

    def clear_log(self):
        self.output_text.config(state='normal')
        self.output_text.delete(1.0, tk.END)
        self.output_text.config(state='disabled')

    def drop_files(self, event):
        if event.data:
            files = self.root.tk.splitlist(event.data)
            for f in files:
                if f.lower().endswith(".csv"):
                    if f not in self.input_files:
                        self.input_files.append(f)
                        self.input_listbox.insert(tk.END, f)
                    self.suggest_output_file()
                else:
                    messagebox.showerror("Invalid File", f"Only CSV files are allowed:\n{f}")

    def create_input_output_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Input / Output")

        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)

        # ============================
        # Input Frame
        # ============================
        input_frame = ttk.LabelFrame(tab, text="Input Files")
        input_frame.grid(row=0, column=0, columnspan=3, padx=10, pady=10, sticky="nsew")

        input_frame.columnconfigure(0, weight=1)
        input_frame.rowconfigure(0, weight=1)

        # Listbox
        self.input_listbox = tk.Listbox(input_frame, height=20, width=50, selectmode='extended')
        self.input_listbox.drop_target_register(DND_FILES)
        self.input_listbox.dnd_bind('<<Drop>>', self.drop_files)
        self.input_listbox.grid(
            row=0, column=0,
            padx=10,
            pady=(10, 4),  # top padding, small gap to buttons
            sticky="nsew"
        )

        # Button row
        button_frame = ttk.Frame(input_frame)
        button_frame.grid(
            row=1, column=0,
            padx=10,
            pady=(2, 10),  # small gap above, bottom padding
            sticky="ew"
        )
        input_frame.columnconfigure(0, weight=1)

        browse_input = ttk.Button(button_frame, text="Browse", command=self.browse_input)
        browse_input.pack(side="left", padx=(0, 10))

        remove_input = ttk.Button(button_frame, text="Remove Selected", command=self.remove_selected_input)
        remove_input.pack(side="left", padx=(0, 10))

        # ============================
        # Output Frame
        # ============================
        output_frame = ttk.LabelFrame(tab, text="Output File")
        output_frame.grid(row=2, column=0, columnspan=3, padx=10, pady=10, sticky="nsew")

        output_frame.columnconfigure(0, weight=1)

        tk.Entry(output_frame, state='readonly', textvariable=self.output_file, width=50).grid(
            row=0, column=0, padx=10, pady=10, sticky="ew"
        )

        browse_output = ttk.Button(output_frame, text="Browse", command=self.browse_output)
        browse_output.grid(row=0, column=1, padx=(0, 10), pady=10, sticky='w')

    def create_field_mapping_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Field Mapping")

        tab.columnconfigure(1, weight=1)

        fields = [
            ("IP Field:", self.ip_field),
            ("MFrom Field:", self.mfrom_field),
            ("HFrom Field:", self.hfrom_field),
            ("Rcpts Field:", self.rcpts_field),
            ("RPath Field:", self.rpath_field),
            ("MsgID Field:", self.msgid_field),
            ("Subject Field:", self.subject_field),
            ("MsgSz Field:", self.msgsz_field),
            ("Date Field:", self.date_field),
            ("Date Format:", self.date_format),  # ⬅️ moved here
        ]

        for i, (label, var) in enumerate(fields):
            tk.Label(tab, text=label).grid(row=i, column=0, sticky=tk.W, padx=5, pady=5)
            tk.Entry(tab, textvariable=var, width=50).grid(
                row=i, column=1, padx=5, pady=5, sticky='ew'
            )

    def create_reporting_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Reporting")

        checkboxes = [
            ("Generate HFrom Report", self.gen_hfrom),
            ("Generate RPath Report", self.gen_rpath),
            ("Generate Alignment Report", self.gen_alignment),
            ("Generate MsgID Report", self.gen_msgid)
        ]

        for i, (label, var) in enumerate(checkboxes):
            tk.Checkbutton(tab, text=label, variable=var).grid(row=i, column=0, sticky=tk.W, padx=5, pady=5)

    def create_parsing_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Parsing")

        checkboxes = [
            ("Expand Recipients", self.expand_recipients),
            ("No Display Name", self.no_display),
            ("Remove PRVS", self.remove_prvs),
            ("Decode SRS", self.decode_srs),
            ("Normalize Bounces", self.normalize_bounces),
            # ("Normalize Entropy", self.normalize_entropy),
            ("No Empty HFrom", self.no_empty_hfrom),
            ("Sample Subject", self.sample_subject),
            ("Exclude Duplicate MsgIDs", self.exclude_dup_msgids)
        ]

        for i, (label, var) in enumerate(checkboxes):
            tk.Checkbutton(tab, text=label, variable=var).grid(row=i, column=0, sticky=tk.W, padx=5, pady=5)

    def create_filters_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Input Filters")

        tab.columnconfigure(0, weight=1)

        # ============================
        # Advanced Options
        # ============================
        options_frame = ttk.LabelFrame(tab, text="Advanced Options")
        options_frame.grid(row=0, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)

        # Two columns for side-by-side layout
        options_frame.columnconfigure(0, weight=1)
        options_frame.columnconfigure(1, weight=1)

        ttk.Checkbutton(
            options_frame,
            text="Don't use default excluded domains",
            variable=self.no_default_exclude_domains
        ).grid(row=0, column=0, sticky="w", padx=5, pady=5)

        ttk.Checkbutton(
            options_frame,
            text="Don't use default excluded IPs",
            variable=self.no_default_exclude_ips
        ).grid(row=0, column=1, sticky="w", padx=5, pady=5)

        # ============================
        # IP / Domain Filters
        # ============================
        filters_frame = ttk.LabelFrame(tab, text="IP / Domain Filters")
        filters_frame.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        filters_frame.columnconfigure(1, weight=1)

        # Exclude IPs
        tk.Label(filters_frame, text="Exclude IPs:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.exclude_ips_entry = tk.Entry(filters_frame, width=30)
        self.exclude_ips_entry.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        add_ip = ttk.Button(filters_frame, text="Add", command=self.add_exclude_ip)
        add_ip.grid(row=0, column=2, padx=5, pady=5)
        self.exclude_ips_listbox = tk.Listbox(filters_frame, height=3, width=50)
        self.exclude_ips_listbox.grid(row=1, column=1, padx=5, pady=5, sticky='nsew')
        remove_ip = ttk.Button(filters_frame, text="Remove Selected", command=self.remove_selected_exclude_ip)
        remove_ip.grid(row=1, column=2, padx=5, pady=5)

        # Exclude Domains
        tk.Label(filters_frame, text="Exclude Domains:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.exclude_domains_entry = tk.Entry(filters_frame, width=30)
        self.exclude_domains_entry.grid(row=2, column=1, padx=5, pady=5, sticky='ew')
        add_domain = ttk.Button(filters_frame, text="Add", command=self.add_exclude_domain)
        add_domain.grid(row=2, column=2, padx=5, pady=5)
        self.exclude_domains_listbox = tk.Listbox(filters_frame, height=3, width=50)
        self.exclude_domains_listbox.grid(row=3, column=1, padx=5, pady=5, sticky='nsew')
        remove_domain = ttk.Button(filters_frame, text="Remove Selected", command=self.remove_selected_exclude_domain)
        remove_domain.grid(row=3, column=2, padx=5, pady=5)

        # Restrict Domains
        tk.Label(filters_frame, text="Restrict Domains:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        self.restrict_domains_entry = tk.Entry(filters_frame, width=30)
        self.restrict_domains_entry.grid(row=4, column=1, padx=5, pady=5, sticky='ew')
        add_restrict = ttk.Button(filters_frame, text="Add", command=self.add_restrict_domain)
        add_restrict.grid(row=4, column=2, padx=5, pady=5)
        self.restrict_domains_listbox = tk.Listbox(filters_frame, height=3, width=50)
        self.restrict_domains_listbox.grid(row=5, column=1, padx=5, pady=5, sticky='nsew')
        remove_restrict = ttk.Button(filters_frame, text="Remove Selected",
                                     command=self.remove_selected_restrict_domain)
        remove_restrict.grid(row=5, column=2, padx=5, pady=5)

        # ============================
        # Sender Filters
        # ============================
        senders_frame = ttk.LabelFrame(tab, text="Sender Filters")
        senders_frame.grid(row=2, column=0, columnspan=3, sticky="nsew", padx=5, pady=5)
        senders_frame.columnconfigure(1, weight=1)

        tk.Label(senders_frame, text="Exclude Senders:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.exclude_senders_entry = tk.Entry(senders_frame, width=30)
        self.exclude_senders_entry.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        add_sender = ttk.Button(senders_frame, text="Add", command=self.add_exclude_sender)
        add_sender.grid(row=0, column=2, padx=5, pady=5)
        self.exclude_senders_listbox = tk.Listbox(senders_frame, height=3, width=50)
        self.exclude_senders_listbox.grid(row=1, column=1, padx=5, pady=5, sticky='nsew')
        remove_sender = ttk.Button(senders_frame, text="Remove Selected", command=self.remove_selected_exclude_sender)
        remove_sender.grid(row=1, column=2, padx=5, pady=5)

    def browse_input(self):
        files = filedialog.askopenfilenames(
            title="Select Input Files",
            filetypes=[("CSV files", "*.csv")],  # ⬅ only CSV allowed
        )
        if files:
            for f in files:
                if f.lower().endswith(".csv"):
                    self.input_files.append(f)
            self.update_input_listbox()
            self.suggest_output_file()

    def update_input_listbox(self):
        self.input_listbox.delete(0, tk.END)
        for file in self.input_files:
            self.input_listbox.insert(tk.END, file)

    def remove_selected_input(self):
        selected = self.input_listbox.curselection()
        if selected:
            for i in sorted(selected, reverse=True):
                del self.input_files[i]
                self.input_listbox.delete(i)

            if not self.input_files and not self.output_file_set_manually:
                self.output_file.set('')

    def browse_output(self):
        current_output = self.output_file.get()
        suggested_dir = os.path.expanduser("~")  # Ultimate fallback
        suggested_name = "SenderStats_Output.xlsx"

        # Prioritize current output dir if set and exists
        if current_output and os.path.exists(os.path.dirname(current_output)):
            suggested_dir = os.path.dirname(current_output)
            suggested_name = os.path.basename(current_output)
        elif self.input_files:
            first_input = self.input_files[0]
            suggested_dir = os.path.dirname(first_input)
            base_name = os.path.splitext(os.path.basename(first_input))[0]
            suggested_name = f"{base_name}_SenderStats.xlsx"

        file = filedialog.asksaveasfilename(
            title="Save Output File",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialdir=suggested_dir,
            initialfile=suggested_name
        )
        if file:
            self.output_file.set(file)
            self.output_file_set_manually = True

    def suggest_output_file(self):
        if self.output_file_set_manually:  # Skip if user set manually
            return
        if not self.input_files:  # Nothing to suggest from
            return

        first_input = self.input_files[0]
        input_dir = os.path.dirname(first_input)
        base_name = os.path.splitext(os.path.basename(first_input))[0]
        suggested_name = f"{base_name}_SenderStats.xlsx"
        suggested_path = os.path.join(input_dir, suggested_name)

        # Append timestamp for uniqueness
        if os.path.exists(suggested_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suggested_name = f"{base_name}_SenderStats_{timestamp}.xlsx"
            suggested_path = os.path.join(input_dir, suggested_name)

        self.output_file.set(suggested_path)

    def add_exclude_ip(self):
        ip = self.exclude_ips_entry.get().strip()
        if ip:
            try:
                is_valid_ip_syntax(ip)
                self.exclude_ips.append(ip)
                self.exclude_ips_listbox.insert(tk.END, ip)
                self.exclude_ips_entry.delete(0, tk.END)
            except ValueError as e:
                messagebox.showerror("Invalid IP", str(e))

    def remove_selected_exclude_ip(self):
        selected = self.exclude_ips_listbox.curselection()
        if selected:
            del self.exclude_ips[selected[0]]
            self.exclude_ips_listbox.delete(selected)

    def add_exclude_domain(self):
        domain = self.exclude_domains_entry.get().strip()
        if domain:
            try:
                is_valid_domain_syntax(domain)
                self.exclude_domains.append(domain)
                self.exclude_domains_listbox.insert(tk.END, domain)
                self.exclude_domains_entry.delete(0, tk.END)
            except ValueError as e:
                messagebox.showerror("Invalid Domain", str(e))

    def remove_selected_exclude_domain(self):
        selected = self.exclude_domains_listbox.curselection()
        if selected:
            del self.exclude_domains[selected[0]]
            self.exclude_domains_listbox.delete(selected)

    def add_restrict_domain(self):
        domain = self.restrict_domains_entry.get().strip()
        if domain:
            try:
                is_valid_domain_syntax(domain)
                self.restrict_domains.append(domain)
                self.restrict_domains_listbox.insert(tk.END, domain)
                self.restrict_domains_entry.delete(0, tk.END)
            except ValueError as e:
                messagebox.showerror("Invalid Domain", str(e))

    def remove_selected_restrict_domain(self):
        selected = self.restrict_domains_listbox.curselection()
        if selected:
            del self.restrict_domains[selected[0]]
            self.restrict_domains_listbox.delete(selected)

    def add_exclude_sender(self):
        sender = self.exclude_senders_entry.get().strip()
        if sender:
            try:
                is_valid_email_syntax(sender)
                self.exclude_senders.append(sender)
                self.exclude_senders_listbox.insert(tk.END, sender)
                self.exclude_senders_entry.delete(0, tk.END)
            except ValueError as e:
                messagebox.showerror("Invalid Email", str(e))

    def remove_selected_exclude_sender(self):
        selected = self.exclude_senders_listbox.curselection()
        if selected:
            del self.exclude_senders[selected[0]]
            self.exclude_senders_listbox.delete(selected)

    def run_tool(self):
        try:
            # Validate required
            if not self.input_files:
                raise ValueError("Input files are required.")
            output = self.output_file.get()
            if not output:
                raise ValueError("Output file is required.")
            validate_xlsx_file(output)

            self.run_button.config(state='disabled', text="Running...")

            # Clear output text
            self.output_text.config(state='normal')
            self.output_text.delete(1.0, tk.END)
            self.output_text.config(state='disabled')

            # Create args namespace
            args = SimpleNamespace()
            args.source_type = DataSourceType.CSV
            args.input_files = self.input_files
            args.token = None
            args.cluster_id = None
            args.output_file = output
            args.ip_field = self.ip_field.get() or DEFAULT_IP_FIELD
            args.mfrom_field = self.mfrom_field.get() or DEFAULT_MFROM_FIELD
            args.hfrom_field = self.hfrom_field.get() or DEFAULT_HFROM_FIELD
            args.rcpts_field = self.rcpts_field.get() or DEFAULT_RCPTS_FIELD
            args.rpath_field = self.rpath_field.get() or DEFAULT_RPATH_FIELD
            args.msgid_field = self.msgid_field.get() or DEFAULT_MSGID_FIELD
            args.subject_field = self.subject_field.get() or DEFAULT_SUBJECT_FIELD
            args.msgsz_field = self.msgsz_field.get() or DEFAULT_MSGSZ_FIELD
            args.date_field = self.date_field.get() or DEFAULT_DATE_FIELD
            args.gen_hfrom = self.gen_hfrom.get()
            args.gen_rpath = self.gen_rpath.get()
            args.gen_alignment = self.gen_alignment.get()
            args.gen_msgid = self.gen_msgid.get()
            args.expand_recipients = self.expand_recipients.get()
            args.no_display = self.no_display.get()
            args.remove_prvs = self.remove_prvs.get()
            args.decode_srs = self.decode_srs.get()
            args.normalize_bounces = self.normalize_bounces.get()
            args.normalize_entropy = self.normalize_entropy.get()
            args.no_empty_hfrom = self.no_empty_hfrom.get()
            args.sample_subject = self.sample_subject.get()
            args.exclude_ips = self.exclude_ips
            args.exclude_domains = self.exclude_domains
            args.restrict_domains = self.restrict_domains
            args.exclude_senders = self.exclude_senders
            args.exclude_dup_msgids = self.exclude_dup_msgids.get()
            args.date_format = self.date_format.get() or DEFAULT_DATE_FORMAT
            args.no_default_exclude_domains = self.no_default_exclude_domains.get()
            args.no_default_exclude_ips = self.no_default_exclude_ips.get()

            def process():
                q_output = QueueOutput(self.result_queue)

                try:
                    # Everything printed here goes into the queue
                    with redirect_stdout(q_output), redirect_stderr(q_output):
                        config = ConfigManager(args)
                        config.display_filter_criteria()

                        data_source_manager = DataSourceManager(config)
                        pipeline_manager = PipelineManager(config)
                        processor = PipelineProcessor(data_source_manager, pipeline_manager)
                        processor.process_data()
                        pipeline_manager.get_filter_manager().display_summary()

                        report = PipelineProcessorReport(config.output_file, pipeline_manager)
                        report.generate()
                        report.close()

                    # optional: flush (no-op in your QueueOutput, but harmless)
                    q_output.flush()
                    self.result_queue.put(("success", None))

                except Exception as e:
                    try:
                        q_output.flush()
                    finally:
                        self.result_queue.put(("error", str(e)))

            threading.Thread(target=process, daemon=True).start()

        except ValueError as e:
            messagebox.showerror("Validation Error", str(e))
            self.run_button.config(state='normal', text="Run SenderStats")

    def load_settings(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    settings = json.load(f)
                # StringVars
                self.ip_field.set(settings.get('ip_field', DEFAULT_IP_FIELD))
                self.mfrom_field.set(settings.get('mfrom_field', DEFAULT_MFROM_FIELD))
                self.hfrom_field.set(settings.get('hfrom_field', DEFAULT_HFROM_FIELD))
                self.rcpts_field.set(settings.get('rcpts_field', DEFAULT_RCPTS_FIELD))
                self.rpath_field.set(settings.get('rpath_field', DEFAULT_RPATH_FIELD))
                self.msgid_field.set(settings.get('msgid_field', DEFAULT_MSGID_FIELD))
                self.subject_field.set(settings.get('subject_field', DEFAULT_SUBJECT_FIELD))
                self.msgsz_field.set(settings.get('msgsz_field', DEFAULT_MSGSZ_FIELD))
                self.date_field.set(settings.get('date_field', DEFAULT_DATE_FIELD))
                self.date_format.set(settings.get('date_format', DEFAULT_DATE_FORMAT))
                # BooleanVars
                self.gen_hfrom.set(settings.get('gen_hfrom', False))
                self.gen_rpath.set(settings.get('gen_rpath', False))
                self.gen_alignment.set(settings.get('gen_alignment', False))
                self.gen_msgid.set(settings.get('gen_msgid', False))
                self.expand_recipients.set(settings.get('expand_recipients', False))
                self.no_display.set(settings.get('no_display', False))
                self.remove_prvs.set(settings.get('remove_prvs', False))
                self.decode_srs.set(settings.get('decode_srs', False))
                self.normalize_bounces.set(settings.get('normalize_bounces', False))
                self.normalize_entropy.set(settings.get('normalize_entropy', False))
                self.no_empty_hfrom.set(settings.get('no_empty_hfrom', False))
                self.sample_subject.set(settings.get('sample_subject', False))
                self.exclude_dup_msgids.set(settings.get('exclude_dup_msgids', False))
                self.no_default_exclude_domains.set(settings.get('no_default_exclude_domains', False))
                self.no_default_exclude_ips.set(settings.get('no_default_exclude_ips', False))
                # Lists (repopulate internal lists and listboxes)
                self.exclude_ips = settings.get('exclude_ips', [])
                for ip in self.exclude_ips:
                    self.exclude_ips_listbox.insert(tk.END, ip)
                self.exclude_domains = settings.get('exclude_domains', [])
                for domain in self.exclude_domains:
                    self.exclude_domains_listbox.insert(tk.END, domain)
                self.restrict_domains = settings.get('restrict_domains', [])
                for domain in self.restrict_domains:
                    self.restrict_domains_listbox.insert(tk.END, domain)
                self.exclude_senders = settings.get('exclude_senders', [])
                for sender in self.exclude_senders:
                    self.exclude_senders_listbox.insert(tk.END, sender)
            except (json.JSONDecodeError, IOError):
                pass  # Use defaults on error

    def save_settings(self):
        settings = {
            # StringVars
            'ip_field': self.ip_field.get(),
            'mfrom_field': self.mfrom_field.get(),
            'hfrom_field': self.hfrom_field.get(),
            'rcpts_field': self.rcpts_field.get(),
            'rpath_field': self.rpath_field.get(),
            'msgid_field': self.msgid_field.get(),
            'subject_field': self.subject_field.get(),
            'msgsz_field': self.msgsz_field.get(),
            'date_field': self.date_field.get(),
            'date_format': self.date_format.get(),
            # BooleanVars
            'gen_hfrom': self.gen_hfrom.get(),
            'gen_rpath': self.gen_rpath.get(),
            'gen_alignment': self.gen_alignment.get(),
            'gen_msgid': self.gen_msgid.get(),
            'expand_recipient': self.expand_recipients.get(),
            'no_display': self.no_display.get(),
            'remove_prvs': self.remove_prvs.get(),
            'decode_srs': self.decode_srs.get(),
            'normalize_bounces': self.normalize_bounces.get(),
            'normalize_entropy': self.normalize_entropy.get(),
            'no_empty_hfrom': self.no_empty_hfrom.get(),
            'sample_subject': self.sample_subject.get(),
            'exclude_dup_msgids': self.exclude_dup_msgids.get(),
            'no_default_exclude_domains': self.no_default_exclude_domains.get(),
            'no_default_exclude_ips': self.no_default_exclude_ips.get(),
            # Lists
            'exclude_ips': self.exclude_ips,
            'exclude_domains': self.exclude_domains,
            'restrict_domains': self.restrict_domains,
            'exclude_senders': self.exclude_senders,
        }
        try:
            with open(self.config_file, 'w') as f:
                json.dump(settings, f, indent=4)
        except IOError:
            pass

    def on_closing(self):
        self.save_settings()
        self.root.destroy()


def main():
    root = TkinterDnD.Tk()
    app = SenderStatsGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

import tkinter as tk
from tkinterdnd2 import DND_FILES, TkinterDnD
from tkinter import filedialog, ttk, messagebox, scrolledtext
from types import SimpleNamespace
import regex as re
import threading
import queue
from contextlib import redirect_stdout
import io

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

class QueueOutput(io.TextIOBase):
    def __init__(self, q):
        self.q = q
        self.buffer = ''

    def write(self, text):
        self.buffer += text
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            self.q.put(("output", line + '\n'))

    def flush(self):
        if self.buffer:
            self.q.put(("output", self.buffer))
            self.buffer = ''

class SenderStatsGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SenderStats")
        self.root.geometry("800x600")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

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
        self.no_display = tk.BooleanVar()
        self.remove_prvs = tk.BooleanVar()
        self.decode_srs = tk.BooleanVar()
        self.normalize_bounces = tk.BooleanVar()
        self.normalize_entropy = tk.BooleanVar()
        self.no_empty_hfrom = tk.BooleanVar()
        self.sample_subject = tk.BooleanVar()
        self.exclude_ips = []
        self.exclude_domains = []
        self.restrict_domains = []
        self.exclude_senders = []
        self.exclude_dup_msgids = tk.BooleanVar()
        self.date_format = tk.StringVar(value=DEFAULT_DATE_FORMAT)
        self.no_default_exclude_domains = tk.BooleanVar(value=False)

        # Notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.grid(row=0, column=0, sticky='nsew')

        self.create_input_output_tab()
        self.create_field_mapping_tab()
        self.create_reporting_tab()
        self.create_parsing_tab()
        self.create_extended_tab()

        log_frame = ttk.LabelFrame(root, text="Log / Status")
        log_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")

        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.output_text = scrolledtext.ScrolledText(log_frame, height=20, state='disabled')
        self.output_text.grid(row=0, column=0, padx=(10,5), pady=10, sticky='nsew')

        self.run_button = tk.Button(root, text="Run SenderStats", command=self.run_tool)
        self.run_button.grid(row=2, column=0, sticky='ew')

    def drop_files(self, event):
        if event.data:
            files = self.root.tk.splitlist(event.data)
            for f in files:
                if f not in self.input_files:
                    self.input_files.append(f)
                    self.input_listbox.insert(tk.END, f)

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
        self.input_listbox = tk.Listbox(input_frame, height=30, width=50, selectmode='extended')
        self.input_listbox.drop_target_register(DND_FILES)
        self.input_listbox.dnd_bind('<<Drop>>', self.drop_files)
        self.input_listbox.grid(row=0, column=0, padx=10, pady=(10,0), sticky="nsew")

        # Button row
        button_frame = ttk.Frame(input_frame)
        button_frame.grid(row=1, column=0, padx=10, pady=10, sticky="w")

        browse_input = tk.Button(button_frame, text="Browse", command=self.browse_input)
        browse_input.pack(side="left", padx=(0,10))

        remove_input = tk.Button(button_frame, text="Remove Selected", command=self.remove_selected_input)
        remove_input.pack(side="left", padx=(0,10))

        # ============================
        # Output Frame
        # ============================
        output_frame = ttk.LabelFrame(tab, text="Output File")
        output_frame.grid(row=2, column=0, columnspan=3, padx=10, pady=10, sticky="nsew")

        output_frame.columnconfigure(0, weight=1)

        tk.Entry(output_frame, textvariable=self.output_file, width=50).grid(
            row=0, column=0, padx=10, pady=10, sticky="ew"
        )

        browse_output = tk.Button(output_frame, text="Browse", command=self.browse_output)
        browse_output.grid(row=0, column=1, padx=(0,10))

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
            ("Date Field:", self.date_field)
        ]

        for i, (label, var) in enumerate(fields):
            tk.Label(tab, text=label).grid(row=i, column=0, sticky=tk.W, padx=5, pady=5)
            tk.Entry(tab, textvariable=var, width=50).grid(row=i, column=1, padx=5, pady=5, sticky='ew')

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
            #("Normalize Entropy", self.normalize_entropy),
            ("No Empty HFrom", self.no_empty_hfrom),
            ("Sample Subject", self.sample_subject)
        ]

        for i, (label, var) in enumerate(checkboxes):
            tk.Checkbutton(tab, text=label, variable=var).grid(row=i, column=0, sticky=tk.W, padx=5, pady=5)

    def create_extended_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Extended")

        tab.columnconfigure(1, weight=1)

        # Exclude IPs
        tk.Label(tab, text="Exclude IPs:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.exclude_ips_entry = tk.Entry(tab, width=30)
        self.exclude_ips_entry.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        add_ip = tk.Button(tab, text="Add", command=self.add_exclude_ip)
        add_ip.grid(row=0, column=2, padx=5, pady=5)
        self.exclude_ips_listbox = tk.Listbox(tab, height=3, width=50)
        self.exclude_ips_listbox.grid(row=1, column=1, padx=5, pady=5, sticky='nsew')
        remove_ip = tk.Button(tab, text="Remove Selected", command=self.remove_selected_exclude_ip)
        remove_ip.grid(row=1, column=2, padx=5, pady=5)

        # Exclude Domains
        tk.Label(tab, text="Exclude Domains:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.exclude_domains_entry = tk.Entry(tab, width=30)
        self.exclude_domains_entry.grid(row=2, column=1, padx=5, pady=5, sticky='ew')
        add_domain = tk.Button(tab, text="Add", command=self.add_exclude_domain)
        add_domain.grid(row=2, column=2, padx=5, pady=5)
        self.exclude_domains_listbox = tk.Listbox(tab, height=3, width=50)
        self.exclude_domains_listbox.grid(row=3, column=1, padx=5, pady=5, sticky='nsew')
        remove_domain = tk.Button(tab, text="Remove Selected", command=self.remove_selected_exclude_domain)
        remove_domain.grid(row=3, column=2, padx=5, pady=5)

        # Restrict Domains
        tk.Label(tab, text="Restrict Domains:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        self.restrict_domains_entry = tk.Entry(tab, width=30)
        self.restrict_domains_entry.grid(row=4, column=1, padx=5, pady=5, sticky='ew')
        add_restrict = tk.Button(tab, text="Add", command=self.add_restrict_domain)
        add_restrict.grid(row=4, column=2, padx=5, pady=5)
        self.restrict_domains_listbox = tk.Listbox(tab, height=3, width=50)
        self.restrict_domains_listbox.grid(row=5, column=1, padx=5, pady=5, sticky='nsew')
        remove_restrict = tk.Button(tab, text="Remove Selected", command=self.remove_selected_restrict_domain)
        remove_restrict.grid(row=5, column=2, padx=5, pady=5)

        # Exclude Senders
        tk.Label(tab, text="Exclude Senders:").grid(row=6, column=0, sticky=tk.W, padx=5, pady=5)
        self.exclude_senders_entry = tk.Entry(tab, width=30)
        self.exclude_senders_entry.grid(row=6, column=1, padx=5, pady=5, sticky='ew')
        add_sender = tk.Button(tab, text="Add", command=self.add_exclude_sender)
        add_sender.grid(row=6, column=2, padx=5, pady=5)
        self.exclude_senders_listbox = tk.Listbox(tab, height=3, width=50)
        self.exclude_senders_listbox.grid(row=7, column=1, padx=5, pady=5, sticky='nsew')
        remove_sender = tk.Button(tab, text="Remove Selected", command=self.remove_selected_exclude_sender)
        remove_sender.grid(row=7, column=2, padx=5, pady=5)

        # Other options
        tk.Checkbutton(tab, text="Exclude Duplicate MsgIDs", variable=self.exclude_dup_msgids).grid(row=8, column=0, sticky=tk.W, padx=5, pady=5)
        tk.Label(tab, text="Date Format:").grid(row=9, column=0, sticky=tk.W, padx=5, pady=5)
        tk.Entry(tab, textvariable=self.date_format, width=50).grid(row=9, column=1, padx=5, pady=5, sticky='ew')
        tk.Checkbutton(tab, text="No Default Exclude Domains", variable=self.no_default_exclude_domains).grid(row=10, column=0, sticky=tk.W, padx=5, pady=5)

    def browse_input(self):
        files = filedialog.askopenfilenames(title="Select Input Files")
        if files:
            self.input_files.extend(files)
            self.update_input_listbox()

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

    def browse_output(self):
        file = filedialog.asksaveasfilename(title="Save Output File", defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
        if file:
            self.output_file.set(file)

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

    def check_queue(self, q):
        while True:
            try:
                msg, arg = q.get_nowait()
                if msg == "success":
                    self.run_button.config(state='normal')
                    return  # Stop checking after success
                elif msg == "error":
                    messagebox.showerror("Unexpected Error", arg)
                    self.run_button.config(state='normal')
                    return  # Stop checking after error
                elif msg == "output":
                    self.output_text.config(state='normal')
                    self.output_text.insert(tk.END, arg)
                    self.output_text.config(state='disabled')
                    self.output_text.see(tk.END)
            except queue.Empty:
                break
        self.root.after(100, self.check_queue, q)

    def run_tool(self):
        try:
            # Validate required
            if not self.input_files:
                raise ValueError("Input files are required.")
            output = self.output_file.get()
            if not output:
                raise ValueError("Output file is required.")
            validate_xlsx_file(output)

            self.run_button.config(state='disabled')

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

            result_queue = queue.Queue()

            def process():
                try:
                    q_output = QueueOutput(result_queue)
                    with redirect_stdout(q_output):
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

                    q_output.flush()
                    result_queue.put(("success", None))
                except Exception as e:
                    q_output.flush()
                    result_queue.put(("error", str(e)))

            threading.Thread(target=process).start()
            self.check_queue(result_queue)

        except ValueError as e:
            messagebox.showerror("Validation Error", str(e))

if __name__ == "__main__":
    root = TkinterDnD.Tk()
    app = SenderStatsGUI(root)
    root.mainloop()
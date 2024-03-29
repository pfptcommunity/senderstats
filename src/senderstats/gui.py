import tkinter as tk
from tkinter import filedialog, messagebox

def select_files():
    file_paths = filedialog.askopenfilenames(title="Select Input Files")
    # Clear the listbox before adding new files
    input_files_listbox.delete(0, tk.END)
    for file_path in file_paths:
        input_files_listbox.insert(tk.END, file_path)

def save_file():
    file_path = filedialog.asksaveasfilename(defaultextension=".txt", filetypes=[("Text files", "*.txt")])
    output_file_var.set(file_path)

def process_data():
    # This is where you can add the logic to process your data
    # For demonstration, it just shows a messagebox
    messagebox.showinfo("Information", "Data processing would happen here.")

# Create the main window
root = tk.Tk()
root.title("Data Processing Dialog")

# Variables
output_file_var = tk.StringVar()

# Input Files
tk.Label(root, text="Input Files:").grid(row=0, column=0, sticky="ne")
input_files_frame = tk.Frame(root)
input_files_frame.grid(row=0, column=1)
input_files_scrollbar = tk.Scrollbar(input_files_frame, orient="vertical")
input_files_listbox = tk.Listbox(input_files_frame, width=100, height=15, yscrollcommand=input_files_scrollbar.set)
input_files_scrollbar.config(command=input_files_listbox.yview)
input_files_listbox.pack(side="left", fill="y")
input_files_scrollbar.pack(side="right", fill="y")
tk.Button(root, text="Select", command=select_files).grid(row=0, column=2, sticky="sew")

# (Omitted the rest of the GUI setup for brevity)

root.mainloop()
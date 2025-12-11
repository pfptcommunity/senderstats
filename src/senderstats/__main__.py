import sys
from senderstats.cli import main as cli_main
from senderstats.gui import main as gui_main

def main():
    if "--gui" in sys.argv:
        sys.argv.remove("--gui")
        gui_main()
    else:
        cli_main()

if __name__ == "__main__":
    main()
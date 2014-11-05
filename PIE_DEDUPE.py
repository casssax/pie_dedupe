""" 
    This is a generic shell for reading in a file and processing it. 
    change 'funct_name' to name of function in 'sub_scripts'
    change 'file_ext' to file name extention (i.e. '.txt', '.csv')
""" 
import sys
import traceback
from ctypes import *
import Scripts.sub_scripts as sub

funct_name = 'pie_dedupe'
file_ext = '.rpt'
##########################################################################################
def strip_suffix(name):  #strip suffix to create output file name w/'.csv'
    return name[:-4]


def do_file(args):
    sawerror = False
    if len(args) == 1:
        windll.user32.MessageBoxA(None,"Error: Please drag at least one file",funct_name,0)
        sys.exit(1)
    try: 
        for fname in args[1:]:
            with open(fname, 'r') as input_file, open(strip_suffix(fname) + file_ext, 'w') as out_file:
                out_file.write('Input file name: ' + fname + '\n')
                funct(input_file, out_file)
        windll.user32.MessageBoxA(None,"Finished",funct_name,0)
    except:
        traceback.print_exc()
        print "Errors occurred, please check the above messages"
        windll.user32.MessageBoxA(None,"Error: Problems occurred, please check them and try again",funct_name,0)

##########################################################################################

if __name__ == "__main__":
    funct = getattr(sub, funct_name)
    do_file(sys.argv)


import subprocess
import tempfile
import xml.etree.ElementTree as ET

def stack_frame_from_xml(xml_stack):
    st = []
    frames = xml_stack.findall('frame')
    for f in frames:
        lineno = None
        directory = None
        filename = None
        lineno_xml = f.find('line')
        fullpath = None
        if lineno_xml != None:
            lineno = int(lineno_xml.text)
        directory_xml = f.find('dir')
        if directory_xml != None:
            directory = directory_xml.text
        filename_xml = f.find('file')
        if filename_xml != None:
            filename = filename_xml.text
        if directory != None and file != None:
            fullpath = directory + "/" + filename
        else:
            obj_xml = f.find('obj')
            assert obj_xml != None
            fullpath = obj_xml.text
        if lineno == None:
            ip_xml = f.find('ip')
            assert ip_xml != None
            lineno = int(ip_xml.text, 16)
        if fullpath != None and lineno != None: 
            fullid = fullpath + ":" + str(lineno)
            st.append(fullid)
    return st

def stack_from_xml(filename):
    tree = None
    try:
        tree = ET.parse(filename)
    # Sometimes, Valgrind got interrupted?
    except ET.ParseError:
        print "Bad valgrind file for {}".format(filename)
        return None
    root = tree.getroot()
    # Find the first error and get that stack.
    errors = {}
    xml_errors = root.findall('error')
    for e in xml_errors:
        u = int(e.find('unique').text, 16)
        s = stack_frame_from_xml(e.find('stack'))
        if s != None:
            errors[u] = s

    # Find the fatal_error, if present, and get that stack.
    xml_fatal_error = root.find('fatal_signal')
    fatal_error = None
    if xml_fatal_error != None:
        fatal_error = stack_frame_from_xml(xml_fatal_error.find('stack'))

    # If we have a fatal_error stack, return that. Otherwise, return
    # the first_error stack.

    if fatal_error != None:
        return fatal_error
    else:
        ekeys = errors.keys()
        ekeys.sort()
        if len(ekeys) == 0:
            return None
        else:
            return errors[ekeys[-1]]

def run(program, arguments, stdin_f=None):
    """
    Take program and arguments, run under valgrind. Parse the error 
    context if present, return it. If stdin is supplied, give that as
    stdin to the child process. Return a tuple of (boolean, string) 
    where the first boolean is whether or not there was a fault, and 
    the second string is the call stack. 
    """
    xmlout = tempfile.NamedTemporaryFile()
    # Put together the command line.  
    cmdline = [ 'valgrind', '--xml=yes', '--xml-file={}'.format(xmlout.name) ] 
    cmdline.append(program)
    cmdline.extend(arguments)
    stdinf = None
    if stdin_f != None:
        stdinf = open(stdin_f, 'r')

    null_out = open('/dev/null', 'w')
    u = subprocess.call(cmdline, stdin=stdinf, stdout=null_out, stderr=null_out)
    st = stack_from_xml(xmlout.name)
    res = True
    if st == None:
        res = True
        st = []
    return (res,"-".join(st))

if __name__ == '__main__':
    # Some tests. 
    rv = run("/home/andrew/code/binutils-2.26-new/binutils/cxxfilt", [], "./afl/8/outdir/crashes/id:000417,sig:11,src:005525,op:havoc,rep:8")
    print rv

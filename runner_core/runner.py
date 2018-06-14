import subprocess
import tempfile
import shutil
import stat
import os
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

def stack_from_xml(data):
    root = None
    try:
        root = ET.fromstring(data)
    # Sometimes, Valgrind got interrupted?
    except ET.ParseError:
        return None
    #root = tree.getroot()
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
    # Create a temporary directory where job-related data will be stored. 
    tempdir = tempfile.mkdtemp()

    # First, we need to go through and find file URIs in 'arguments', and 
    # make them available in the Docker container. 
    real_program = program
    if program[:7] == "file://":
        p = program[7:]
        pbase = os.path.basename(p)
        shutil.copy(p, "{0}/{1}".format(tempdir, pbase))
        real_program = "/sandbox/{}".format(pbase)

    converted_arguments = []
    for a in arguments:
        if a[:7] == "file://":
            b = a[7:]
            bbase = os.path.basename(b)
            shutil.copy(b, "{0}/{1}".format(tempdir, bbase))
            converted_arguments.append("/sandbox/{1}".format(bbase))
        else:
            converted_arguments.append(a)

    if stdin_f != None:
        if stdin_f[:7] == "file://":
            b = stdin_f[7:]
            bbase = os.path.basename(b)
            shutil.copy(b, "{0}/{1}".format(tempdir, bbase))
            stdin_f = "/sandbox/{0}".format(bbase)

    # Then, create the Docker command line.
    docker_cmdline = ["docker", "run", "--rm", "--user", "1000"]
    docker_cmdline.append("-v")
    docker_cmdline.append("{}:/sandbox".format(tempdir))
    docker_cmdline.append("grinder")

    # Then, create the Valgrind command line.
    valgrind_cmdline = ["/usr/bin/valgrind", "--xml=yes", "--xml-file=/sandbox/out.xml"]
    valgrind_cmdline.append(real_program)

    for a in arguments:
        valgrind_cmdline.append(a)
   
    valgrind_cmd = " ".join(valgrind_cmdline)
    if stdin_f != None:
        valgrind_cmd = "{0} < {1}".format(valgrind_cmd, stdin_f)

    timeout_cmd = "timeout 5m {}".format(valgrind_cmd)
    runsh = open("{}/run.sh".format(tempdir), "w")
    runsh.write("#!/bin/bash\n{}\n".format(timeout_cmd))
    runsh.close()
    os.chmod("{}/run.sh".format(tempdir), stat.S_IREAD|stat.S_IEXEC)

    # Then, jam them together and run them. 
    docker_cmdline.append("/sandbox/run.sh") 
    null_out = open('/dev/null', 'w')
    #subprocess.call(docker_cmdline, stdout=null_out, stderr=null_out)
    subprocess.call(docker_cmdline)

    # Then, read the XML data file output. 
    data = open("{}/out.xml".format(tempdir), 'r').read()

    # Tear down the temporary directory structure now that we don't need it.
    shutil.rmtree(tempdir)
    return data

if __name__ == '__main__':
    # Some tests. 
    rv = run("file:///home/andrew/code/binutils-2.26-new/binutils/cxxfilt", [], "file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/8/outdir/crashes/id:000417,sig:11,src:005525,op:havoc,rep:8")
    print rv

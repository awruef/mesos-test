import subprocess
import tempfile
import shutil
import glob
import stat
import os
import xml.etree.ElementTree as ET

def stack_from_asan_log(data):
    stack = []
    for line in data.split("\n"):
        k = line.find("#")
        if k != -1:
            k = line.find("in", k)
            stack.append(line[k+2:].rstrip())
    return stack

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

def run2_asan(run_tasks):
    """
    Take a program and a list of arguments and stdins, run them 
    """
    tempdir = tempfile.mkdtemp()
    cmdlines = []
    idx_map = {}
    idx = 0

    for (program,(arguments,stdin_f),outdata) in run_tasks:
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
                shutil.copy(b, "{0}/{1}{2}".format(tempdir, bbase, idx))
                converted_arguments.append("/sandbox/{1}{2}".format(bbase, idx))
            else:
                converted_arguments.append(a)

        if stdin_f != None:
            if stdin_f[:7] == "file://":
                b = stdin_f[7:]
                bbase = os.path.basename(b)
                shutil.copy(b, "{0}/{1}{2}".format(tempdir, bbase, idx))
                stdin_f = "/sandbox/{0}{1}".format(bbase, idx)
        exec_c = []
        logname = "/sandbox/out{i}".format(i=idx)
        exec_c.append(real_program)
        for a in converted_arguments:
            exec_c.append(a)
  
        cmd = " ".join(exec_c)
        if stdin_f != None:
            cmd = "{command} < {stdinput}".format(command=cmd, stdinput=stdin_f)
        idx_map[idx] = outdata
        env = "ASAN_OPTIONS=detect_leaks=false,replace_str=false,log_path={log}".format(log=logname)
        timeout_cmd = "{e} timeout 5m {c}".format(e=env,c=cmd)
        cmdlines.append(timeout_cmd) 
        idx = idx + 1
    
    runsh = open("{}/run.sh".format(tempdir), "w")
    runsh.write("#!/bin/bash\n")
    for l in cmdlines:
        runsh.write("{}\n".format(l))
    runsh.write("exit 0\n")
    runsh.close()
    os.chmod("{}/run.sh".format(tempdir), stat.S_IREAD|stat.S_IEXEC)

    docker_cmdline = ["docker", "run", "--rm", "--user", "1000", "-m", "1024m"]
    docker_cmdline.append("-v")
    docker_cmdline.append("{}:/sandbox".format(tempdir))
    docker_cmdline.append("ubuntu:16.04")
    docker_cmdline.append("/sandbox/run.sh") 
    while True:
        null_out = open('/dev/null', 'w')
        result = subprocess.call(docker_cmdline, stdout=null_out, stderr=null_out)
        if result == 0:
            break
   
    # Gather up all the produced ASAN files and return them
    datas = []
    for i in range(0, len(run_tasks)):
        u = glob.glob("{tdir}/out{idx}.*".format(tdir=tempdir,idx=i))
        if len(u) > 0:
            data = open(u[0], 'r').read()
        else:
            data = ""
        o = idx_map[i]
        o['stack'] = data
        datas.append(o)

    shutil.rmtree(tempdir)
    return datas

def run2(program, arguments_list):
    """
    Take program and a list of arguments and stdins, run them ALL
    sequentially under the SAME docker container. 
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
    
    cmdlines = []
    idx = 0
    for (arguments,stdin_f) in arguments_list:
        converted_arguments = []
        for a in arguments:
            if a[:7] == "file://":
                b = a[7:]
                bbase = os.path.basename(b)
                shutil.copy(b, "{0}/{1}{2}".format(tempdir, bbase, idx))
                converted_arguments.append("/sandbox/{1}{2}".format(bbase, idx))
            else:
                converted_arguments.append(a)

        if stdin_f != None:
            if stdin_f[:7] == "file://":
                b = stdin_f[7:]
                bbase = os.path.basename(b)
                shutil.copy(b, "{0}/{1}{2}".format(tempdir, bbase, idx))
                stdin_f = "/sandbox/{0}{1}".format(bbase, idx)
        valgrind_cmdline = ["/usr/bin/valgrind", "--xml=yes", "--xml-file=/sandbox/out{}.xml".format(idx)]
        valgrind_cmdline.append(real_program)
        for a in converted_arguments:
            valgrind_cmdline.append(a)
   
        valgrind_cmd = " ".join(valgrind_cmdline)
        if stdin_f != None:
            valgrind_cmd = "{0} < {1}".format(valgrind_cmd, stdin_f)

        timeout_cmd = "timeout 5m {}".format(valgrind_cmd)
        cmdlines.append(timeout_cmd) 
       
        idx = idx + 1
    # Write all the cmdlines out into the run.sh file. 
    runsh = open("{}/run.sh".format(tempdir), "w")
    runsh.write("#!/bin/bash\n")
    for l in cmdlines:
        runsh.write("{}\n".format(l))
    runsh.write("exit 0\n")
    runsh.close()
    os.chmod("{}/run.sh".format(tempdir), stat.S_IREAD|stat.S_IEXEC)

    # Run run.sh under docker
    docker_cmdline = ["docker", "run", "--rm", "--user", "1000", "-m", "1024m"]
    docker_cmdline.append("-v")
    docker_cmdline.append("{}:/sandbox".format(tempdir))
    docker_cmdline.append("grinder")
    docker_cmdline.append("/sandbox/run.sh") 
    while True:
        null_out = open('/dev/null', 'w')
        result = subprocess.call(docker_cmdline) #, stdout=null_out, stderr=null_out)
        if result == 0:
            break

    # Gather up all the produced XML files and return them in order. 
    datas = []
    for i in range(0, idx):
        data = open("{0}/out{1}.xml".format(tempdir, i), 'r').read()
        datas.append(data)

    shutil.rmtree(tempdir)
    return datas
 
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

    for a in converted_arguments:
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
    #subprocess.call(docker_cmdline, stdout=null_out)
    print docker_cmdline 
    subprocess.call(docker_cmdline)

    # Then, read the XML data file output. 
    data = open("{}/out.xml".format(tempdir), 'r').read()

    # Tear down the temporary directory structure now that we don't need it.
    shutil.rmtree(tempdir)
    return data

if __name__ == '__main__':
    programs = []
    programs.append('file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/asan_bins/2ea53e003163338a403d5afbb2046cafb8f3abe9/bin/c++filt')
    programs.append('file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/asan_bins/2ea53e003163338a403d5afbb2046cafb8f3abe9/bin/c++filt')
    inputs = []
    inputs.append(([],'file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/2/outdir/crashes/id:000541,sig:11,src:007824,op:ext_AO,pos:38'))
    inputs.append(([],'file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/2/outdir/crashes/id:000543,sig:11,src:007835,op:ext_AO,pos:38'))
    stuff = []
    stuff.append({})
    stuff.append({})
    tasks = zip(programs,inputs,stuff)
    rv = run2_asan(tasks)
    print rv
    # Some tests. 
    #rv = run("file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/bins/eee926f28e8745dcd03adcb1113f3e4a7b79b1e5/bin/c++filt", [], "file:///home//andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/8/outdir/crashes/id:000417,sig:11,src:005525,op:havoc,rep:8")
    #rv = run2("file:///home/andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/bins/eee926f28e8745dcd03adcb1113f3e4a7b79b1e5/bin/c++filt", [([], "file:///home//andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/8/outdir/crashes/id:000417,sig:11,src:005525,op:havoc,rep:8"),([],"file:///home//andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/8/outdir/crashes/id:000410,sig:11,src:005215,op:arith8,pos:29,val:-21"),([],"file:///home//andrew/code/mesos-test-case-runner/cxxfilt-fuzzing/inputs/afl/8/outdir/crashes/id:000413,sig:11,src:005294,op:havoc,rep:4")])
    #print rv

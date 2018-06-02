import subprocess
import xml 

def run(program, arguments, stdin=None):
    """
    Take program and arguments, run under valgrind. Parse the error 
    context if present, return it. If stdin is supplied, give that as
    stdin to the child process. Return a tuple of (boolean, string) 
    where the first boolean is whether or not there was a fault, and 
    the second string is the call stack. 
    """
    
    return None

if __name__ == '__main__':
    # Some tests. 
    pass

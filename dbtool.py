#!/usr/bin/env python2.7
import xml.etree.ElementTree as ET
from Queue import Queue
from progressbar import ProgressBar,Bar,Percentage
import argparse
import threading
import sqlite3
import base64
import copy
import zlib
import sys
import csv

def init(c):
    c1 = """
CREATE TABLE results(
  id INTEGER PRIMARY KEY,
  hash TEXT,
  filepath TEXT,
  fuzzer TEXT,
  run INTEGER,
  vg_status TEXT,
  vg_stack INTEGER);
    """
    c.execute(c1)
    c2 = """   
CREATE TABLE stack(
  id INTEGER PRIMARY KEY,
  frame INTEGER,
  next INTEGER,
  CONSTRAINT suniques UNIQUE(frame,next));
    """
    c.execute(c2)
    c3 = """
CREATE TABLE frames(
  id INTEGER PRIMARY KEY,
  filename TEXT,
  lineno INTEGER,
  CONSTRAINT funiques UNIQUE (filename,lineno));
    """
    c.execute(c3)
    return

def is_init(c):
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results';")
    u = c.fetchall()
    return len(u) != 0

def parse_path(path):
    l = path.split("/")
    return (l[-5],int(l[-4]))

def stack_frame_from_xml(xml_stack):
    st = []
    frames = xml_stack.findall('frame')
    for f in frames:
        lineno = None
        directory = None
        filename = None
        lineno_c = f.find('line')
        if lineno_c != None:
            lineno = int(lineno_c.text)
        directory_c = f.find('dir')
        if directory_c != None:
            directory = directory_c.text
        filename_c = f.find('file')
        if filename_c != None:
            filename = filename_c.text
        if lineno == None or directory == None or filename == None:
            continue
        fullpath = directory + "/" + filename
        fullid = (fullpath,lineno)
        st.append(fullid)
    return st

def stack_from_xml(root):
    # Find the first error and get that stack.
    errors = {}
    res = "NOCRASH"
    xml_errors = root.findall('error')
    for e in xml_errors:
        kind = e.find('kind')
        # Skip leak errors, don't care. 
        if kind != None and kind.text[0:4] == "Leak":
            continue
        # Skip "Fishy values"
        if kind != None and kind.text == "FishyValue":
            continue
        u = int(e.find('unique').text, 16)
        s = stack_frame_from_xml(e.find('stack'))
        errors[u] = s

    # Find the fatal_error, if present, and get that stack.
    xml_fatal_error = root.find('fatal_signal')
    fatal_error = None
    if xml_fatal_error != None:
        fatal_error = stack_frame_from_xml(xml_fatal_error.find('stack'))
        res = "CRASH"

    # If we have a fatal_error stack, return that. Otherwise, return
    # the first_error stack.

    if fatal_error != None:
        return (res,fatal_error)
    else:
        if len(errors) > 0:
            ekeys = errors.keys()
            ekeys.sort()
            return (res,errors[ekeys[0]])
        else:
            return (res,[])

def parse_vgdata(vgdata):
    d = base64.b64decode(vgdata)
    # Check and see if 'd' is XML data or not
    r = None 
    if d.startswith('<'):
        try:
            r = ET.fromstring(d)
        except ET.ParseError:
            return ("UNK", [])
    else:
        tmp = zlib.decompress(d)
        try:
            r = ET.fromstring(tmp)
        except ET.ParseError:
            return ("UNK", [])

    return stack_from_xml(r)

class Entry(object):
    def __init__(self,p,h,n,rn,vst,vsk):
        self.filepath = p
        self.commithash = h
        self.fuzzername = n
        self.runnumber = rn
        self.stack = vsk
        self.vgstatus = vst

def make_entry(line):
    if len(line) > 3:
        print line
    #hsh = line[0]
    #path = line[1]
    #vgdata = line[2]
    hsh,path,vgdata = line
    fuzzername,runnumber = parse_path(path)
    vgstatus,vgstack = parse_vgdata(vgdata)
    return Entry(path,hsh,fuzzername,runnumber,vgstatus,vgstack)

# Insert a stack frame, return the id of the top of the stack.
def insert_stack(cur, stack, framecache, stackcache):
    ls = stack
    ls.reverse()
    first = True
    prev_id = None
    #cur = conn.cursor()
    for l in ls:
        # Insert into frames first. 
        frame_id = framecache.put((l[0],l[1]))
        cur.execute("INSERT OR IGNORE INTO frames(filename,lineno) VALUES(\"{0}\",{1});".format(l[0],l[1]))

        # Do the lookup for the value we just inserted 
        if first:
            first = False
            prev_id = stackcache.get((frame_id,None))
            if prev_id == None:
                cur.execute("INSERT INTO stack(frame) VALUES({0});".format(frame_id))
                prev_id = stackcache.put((frame_id,None))
                assert prev_id != None
        else:
            res = stackcache.get((frame_id,prev_id))
            if res == None:
                cur.execute("INSERT INTO stack(frame,next) VALUES({0},{1});".format(frame_id,prev_id))
                prev_id = stackcache.put((frame_id,prev_id))
                assert prev_id != None
            else:
                prev_id = res
    #conn.commit() 
    return prev_id

def insert_entry(cur, record, framecache, stackcache):
    # First, insert the stack, if present. 
    stack_id = None 
    if len(record.stack) > 0:
        stack_id = insert_stack(cur, record.stack, framecache, stackcache)
    # Then, insert the rest of the record. 
    if stack_id != None:
        cur.execute("INSERT INTO results(hash,filepath,fuzzer,run,vg_status,vg_stack) VALUES(\"{0}\",\"{1}\",\"{2}\",{3},\"{4}\",{5});".format(record.commithash, record.filepath, record.fuzzername, record.runnumber, record.vgstatus, stack_id))
    else:
        cur.execute("INSERT INTO results(hash,filepath,fuzzer,run,vg_status) VALUES(\"{0}\",\"{1}\",\"{2}\",{3},\"{4}\");".format(record.commithash, record.filepath, record.fuzzername, record.runnumber, record.vgstatus))
    return

def insert_record_into_db(line, cur, framecache, stackcache):
    # Parse line out into something we can insert
    entry = make_entry(line)
    insert_entry(cur, entry, framecache, stackcache)
    #conn_lock.release()

def tmain(q, db, conn_lock, framecache, stackcache):
    conn = sqlite3.connect(db)
    while True:
        insert_record_into_db(q.get(), conn, conn_lock, framecache, stackcache)
        q.task_done()

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

class Cache(object):
    def __init__(self):
        self.map = {}
        self.idx = 1

    def get(self, val):
        return self.map.get(val, None)

    def put(self, val):
        r = None
        if self.map.has_key(val):
            r = self.map.get(val)
        else:
            r = self.idx
            self.map[val] = self.idx
            self.idx = self.idx + 1
        return r

def main(args):
    csv.field_size_limit(100000000)
    con = sqlite3.connect(args.database)
    c = con.cursor()
    # Do we need to initialize the database?
    if not is_init(c):
        init(c)
        con.commit()
    else:
        print "Only works on fresh data bases right now"
        return 1

    con.close()
    # Count the lines in the input file. 
    max_lines = file_len(args.runoutput)-1	

    # Read in the CSV file we are translating from
    inf = open(args.runoutput, 'r')
    reader = csv.reader(inf)
    reader.next()

    q = Queue(maxsize=16)
    l = threading.Lock()
    framecache = Cache()
    stackcache = Cache()
    for i in range(0, 1):
        t = threading.Thread(target=tmain, args=(q,args.database,l,framecache,stackcache))
        t.setDaemon(True)
        t.start()

    cur = 0
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=max_lines).start()
    conn = sqlite3.connect(args.database)
    cursor = conn.cursor()
    for line in reader:
        insert_record_into_db(line, cursor, framecache, stackcache)
        #q.put(line)
        cur = cur + 1
        if cur % 1000 == 0:
            conn.commit()
            cursor = conn.cursor()
        pbar.update(cur)
    conn.commit()
    #q.join()
    pbar.finish()
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser('dbtool')
    parser.add_argument('runoutput')
    parser.add_argument('database')
    args = parser.parse_args()
    sys.exit(main(args))

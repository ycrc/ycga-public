'''
Todo
- DONE avoid clobbering project tars if multiple projects of same name exist in tree
- LATER change owner of project tars to pi and pi group, chmod to 440

'''

'''
Data is either pre 1.8 or 1.8.

Pre 1.8:  
  unmapped fastq data is in files named: s_1..export.txt[.gz]
  mapped export format data is in files named: s_1..sequence.txt[.gz]

Both are not always present, since deletion of sequence files was optional, as was mapping.  
This script omits exports if sequence exists.  If no sequence, exports will be saved as is, rather than converted back.

1.8:
  unmapped fastq data is in files named like this: SAG101-1P_GCCAAT-GCCAAT_L006_R2_003.fastq.gz
  they should always be present, by convention only, in Unaligned*/Project_*/Sample_*/

  mapped files may not be present, and are named like this: SAG101-1P_GCCAAT-GCCAAT_L006_R2_003.export.txt.gz
  if present, they are by convention only, in Unaligned*/Project_*/Sample_*/

For pre1.8 runs, we'll aim to make a single tarball with the important small files, plus the fastq files as quip files, and only exports for which there were no
fastqs found.

For 1.8 runs, we'll aim to make a main tarball with important small files.  Then, for each Unaligned*/Project* directory, we'll make a separate tarball of that, so that
data belonging to different users is stored in separate tarballs.

To do a single archive run:

python archive.py [-n] -v -r /ycga-gpfs/sequencers/illumina/sequencerY/runs/161007_K00162_0117_AHFT32BBXX

'''

import os, tarfile, subprocess, logging, argparse, sys, re, tempfile, time, threading, hashlib, gzip, glob, datetime

QUIP='/ycga-gpfs/apps/hpc/Tools/quip/1.1.8/bin/quip'

# Directories matching these patterns, and everything below them, will not be archived
ignoredirs='''Aligned\S*$
oDiag$
Calibration\S*$
EventScripts$
Images$
InterOp$
Logs$
PeriodicSaveRates$
Processed$
Queued$
ReadPrep1$
ReadPrep2$
Recipe$
Temp$
Thumbnail_Images$
DataRTALogs$
Data/TileStatus$
Data/Intensities/L\d*$
Data/Intensities/Offsets$
Data/Intensities/BaseCalls/L\d*$
GERALD\S*/CASAVA\S*$
GERALD\S*/Temp$
Matrix$
Phasing$
Plots$
Stats$
SignalMeans$
L00\d$
'''
ignoreDirsPat=re.compile('|'.join(ignoredirs.split()))

# Files matching these patterns will not be archived
ignorefiles='''s_+.+_anomaly.txt$
s_+.+_reanomraw.txt$
'''
ignoreFilesPat=re.compile('|'.join(ignorefiles.split()))

# Files matching these patterns are unmapped reads, and will be quipped and archived
fastqfiles='''\.fastq\.txt\.gz$
\.fastq$
\.fastq.gz$
\.fq$
\_sequence.txt.gz
'''
fastqFilesPat=re.compile('|'.join(fastqfiles.split()))

# Files matching these patterns are old style mapped reads, and will be archived only if matching unmapped reads are not present
oldexportfiles='''s_\d_.*export.txt.gz$
s_\d_.*export.txt$
'''
oldexportFilesPat=re.compile('|'.join(oldexportfiles.split()))

# quip files will be archived as is
quipfiles='''.qp$'''
quipFilesPat=re.compile('|'.join(quipfiles.split()))

# Directories matching this pattern are 1.8 Unaligned Project directories.  We want to archive their contents to a project-specific tar file.
projectPat=re.compile('Unaligned\S*/Project_[\w\-_]+$')

epilog="epilog"

'''
This function strips off any part of the path that comes before the standard path 
beginning.  For example, '/gpfs/scratch/ycga/data/panfs..' will be reduced to '/panfs..'

If such a prefix isn't found, we just use the path as given to us
'''

def mkarcdir(pth, archivetop):
    i=pth.find('/panfs/sequencers')
    if i != -1: return archivetop+pth[i:]
    i=pth.find('/ycga-ba/ba_sequencers')
    if i != -1: return archivetop+pth[i:]
    i=pth.find('/ycga-gpfs/sequencers/illumina')
    if i != -1: return archivetop+pth[i:]

    if pth.startswith('/'):
        return archivetop+pth
    else:
        return archivetop+'/'+pth

'''
This class wraps a tarfile object.  It adds a couple of bits of functionality:
It keeps track of what's been added and refuses to add the same archive name twice
It keeps a list of validation tasks to be done at the end
'''
class tarwrapper(object):
    def __init__(self, fn):
        self.fn=fn # name of tar file
        self.tfp=tarfile.open(fn, 'w')
        self.added=set() #files already added to archive
        self.check=[] # validation task to run at the end

    ''' 
    name is the actual file holding the data (sometimes a temp file)
    origname is the original file name (full path)
    arcname is the name we want on the archive (full path)
    '''
    def add(self, name, origname=None, arcname=None):
        if not origname:
            origname=name
        if not arcname:
            arcname=name
        if arcname in self.added:
            error("Attempting to overwrite %s in %s" % (arcname, self.fn))
        self.tfp.add(name, arcname)
        self.added.add(arcname)
        self.check.append(validatejob(origname, arcname, o.testlen))

    def validate(self):
        status=True
        self.tfp.close()
        self.tfp=tarfile.open(self.fn, 'r')
        #processJobs(self.check, 1) 
        for c in self.check:
            c.run(self.tfp)
            status &= c.finish()

        self.tfp.close()

        if not status:
            error("Validation failed")


''' utility function '''
def countTrue(*vals):
    cnt=0
    for v in vals:
        if v: cnt+=1
    return cnt

''' This function turns a negative date delta into a 6 digit date that many days in the past.  Anything else is returned unchanged '''
def fixCut(cut):
    try:
        if int(cut) < 0:
            d=datetime.datetime.now() + datetime.timedelta(days=int(cut))
            return d.strftime("%y%m%d")
    except:
        pass
    return cut

''' Utility error function.  Bomb out with an error message and code '''
def error(msg):
    logger.error(msg)
    raise RuntimeError(msg)

'''
Prune the search by removing any directory that matches ignoresPat from the dirs list.
Remove and return any directories that look like: "Unaligned*/Project_*".  We'll handle them separately.
'''
def prunedirs(dirname, dirs):
    projects=[]
    for d in dirs[:]: # use a copy of the list
        dn=dirname+'/'+d
        if ignoreDirsPat.search(dn): 
            logger.debug("pruning %s" % dn)
            dirs.remove(d)
            continue
        if o.projecttars and projectPat.search(dn):
            logger.debug("found project %s" % dn)
            projects.append(d)
            dirs.remove(d)
            continue
    return projects

'''
modify the files list by removing all files that should not simply be added as is to the archive
return a list of fastq files that should be quipped before being added
pre 1.8 exports files will be added only if matching sequence files are missing

    if a file in old exports has an equivalent quip file or fastq file, drop it
    if a file in fastqs has an equivalent in quips, drop it, otherwise create a quip job to turn it into a quip file
    keep any quip file

    when we return, files should be a list of files to add as is
    fastqs should be a list of quip jobs
'''
def handlefiles(dirname, files, tfp):
    fastqs=[]
    quips=[]
    oldexports=[]
    #remove files that can be ignored, and save fastq and old export files.
    for f in files[:]: # use a copy of the list 
        fn=dirname+'/'+f
        if os.path.islink(fn): continue # just tar links as is
        if ignoreFilesPat.search(f): 
            logger.debug("skipping %s" % fn)
            files.remove(f)
        elif quipFilesPat.search(f):
            # we just record the quips, but don't remove from files to archive just yet
            quips.append(fn)
        elif fastqFilesPat.search(f):
            if os.path.getsize(fn) < 1000: 
                logger.debug("found small fastq, not quipping: %s" % fn)
                # this file seems bogus, we'll leave it and just tar it as is.
            else:
                fastqs.append(fn)
                files.remove(f)
        elif oldexportFilesPat.search(f):
            oldexports.append(fn)
            files.remove(f)

    if quips: logger.debug("found quips %s" % " ".join(quips))

    if oldexports: logger.debug("found old exports %s" % " ".join(oldexports))
    # we'll keep oldexports for which there is no fastq or quip (and won't bother converting or compressing it)
    for oe in oldexports[:]:
        # FIX ALL of this
        if oe.replace("export", "sequence", 1) in fastqs or oe.replace("export.txt.gz", "fastq.qp", 1) in quips: 
           logger.debug("skipping old export %s" % oe)
        else:
            logger.debug("keeping old export %s, no sequence or quip file found" % oe)
            files.append(os.path.basename(oe)) # put this export back on the list to archive

    if fastqs: logger.debug("found fastqs %s" % " ".join(fastqs))
    # we'll keep and quip fastqs which there is not currently a quip 
    for ofq in fastqs[:]:
        if ofq.replace("sequence.txt.gz", "fastq.qp", 1) in quips:
           logger.debug("skipping fastq %s, quip found." % ofq)
           fastqs.remove(ofq)
        elif ofq.replace("fastq.gz", "fastq.qp", 1) in quips:
           logger.debug("skipping fastq %s, quip found." % ofq)
           fastqs.remove(ofq)
        else:
            logger.debug("quipping %s" % ofq)

    return [quipjob(ofq, tfp) for ofq in fastqs]

''' utility class to hold various counts'''
class stats(object):
    def __init__(self):
        self.bytes=0
        self.quips=0
        self.files=0
        self.tarfiles=0
        
    def comb(self, other):
        self.bytes+=other.bytes
        self.quips+=other.quips
        self.files+=other.files
        self.tarfiles+=other.tarfiles

''' encapsulates a validation task.  We may parallelize these similar to quipjobs in the future 
Most files are validated by simply comparing the first portion of the file from both the archive and the original.
quipped files are different: The quip file must be dequipped, and the original (usually gzipped) is uncompressed before comparing
We usually only do the first 10000 bytes to save time.
'''
class validatejob(object):
    def __init__(self, fn, tn, testlen=10000):
        self.fn=fn #original name
        self.tn=tn # archive name
        self.testlen=testlen # number of bytes to compare
        self.status=False

    def __str__(self):
        return "Validate Job "+self.fn

    ''' quip removes the second header from fastq files (the 3rd line, beginning with +).  That 
    would cause the validation to fail if the original had the long header, which sometimes happens.  This function converts the 
    fastq to the standard format with ontly + in the 3rd line.
    '''
    def fixFastqFormat(self, s, ln):
        l=s.split('\n')
        for i in range(2,len(l),4):
            assert(not l[i] or l[i][0]=='+')
            if l[i]: l[i]='+'
        return '\n'.join(l)[:ln]

    def run(self, tfp):
        logger.debug("validating %s and %s" % (self.fn, self.tn))
        if os.path.islink(self.fn):
            self.status=True # don't bother to check links
            return
        if (self.fn.endswith(".gz") or self.fn.endswith(".fastq") or self.fn.endswith(".txt")) and self.tn.endswith(".qp"):
            s1=0
            s2=0
            # uncompress both files and compare
            if self.fn.endswith(".gz"):
                #str1orig=gzip.open(self.fn).read(self.testlen*2)
                #str1=self.fixFastqFormat(str1orig, self.testlen)
                h1=hashlib.md5(self.fixFastqFormat(gzip.open(self.fn).read(self.testlen*2), self.testlen)).hexdigest()
            else:
                h1=hashlib.md5(self.fixFastqFormat(open(self.fn).read(self.testlen*2), self.testlen)).hexdigest()
                #str1=self.fixFastqFormat(open(self.fn).read(self.testlen*2), self.testlen)

            #h1=hashlib.md5(str1).hexdigest()

            cmd='tar xf %s -O %s 2> /dev/null | %s -c -i quip -o fastq' % (tfp.name, self.tn, QUIP)
            p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            #str2=p.stdout.read(self.testlen)
            #h2=hashlib.md5(str2).hexdigest()
            h2=hashlib.md5(p.stdout.read(self.testlen)).hexdigest()
            p.terminate()
            self.status=h1==h2
        else:
            # just compare files as is
            h1=hashlib.md5(open(self.fn).read(self.testlen)).hexdigest()
            s1=os.path.getsize(self.fn)
            h2=hashlib.md5(tfp.extractfile(self.tn).read(self.testlen)).hexdigest()
            s2=tfp.getmember(self.tn).size
            self.status = (h1==h2) and (s1==s2)
        if self.status:
            logger.debug("validated %s hash %s status %s" % (self.fn, h1, str(self.status)))
        else:
            logger.error("validated %s h1 %s h2 %s s1 %d s2 %d status %s" % (self.fn, h1, h2, s1, s2, str(self.status)))
        
    def finish(self):
        return self.status

'''
This thread object converts fn to a temp file
'''        
class quipjob(threading.Thread):
    def __init__(self, fn, tarp):
        threading.Thread.__init__(self)
        self.fn=fn
        self.tarp=tarp
        self.tn=re.sub('.gz$', '', fn)+'.qp'
        self.est_sz=os.path.getsize(fn)*0.75 # estimated size of converted file
        self.true_sz=0
        self.check=[]
        self.status=None

    def __str__(self):
        return "Quip Job "+self.fn

    def run(self):
        self.tmpfp=tempfile.NamedTemporaryFile(dir=o.tmpdir)
        cmd='%s -c %s > %s' % (QUIP, self.fn, self.tmpfp.name)
        logger.debug("running %s" % cmd) 
        if not o.dryrun: 
            self.status=subprocess.call(cmd, shell=True)
            if self.status: 
                logger.error("FAILED error %d: %s" % (self.status, cmd))

        self.true_sz=os.path.getsize(self.tmpfp.name)

    def finish(self, runstats):
        if not o.dryrun:
            self.tarp.add(self.tmpfp.name, self.fn, self.tn)

        runstats.bytes+=self.true_sz; runstats.files+=1; runstats.quips+=1
        self.tmpfp.close()

'''
Generic threaded job processor
'''

def processJobs(jobs, maxthds, runstats):

    pending=jobs
    running=[]
    done=[]
    currsum=0 # running total of estimate of memory needed by all running jobs.  We don't want to exceed o.maxsum

    # first, make sure none of the jobs are individually larger than o.maxsum, else we'll deadlock below
    for j in pending:
        if j.est_sz > o.maxsum:
            error("Found a big job %s %d" % (j.fn, j.est_sz))

    while pending or running:
        # start as many jobs simultaneously as allowed by maxthds and maxsum
        while pending and len(running)<maxthds and (currsum+pending[0].est_sz) < o.maxsum:
            job=pending.pop(0)
            currsum += job.est_sz
            job.starttime=time.time()
            logger.debug("starting job %s currsum is %d" % (job, currsum))
            job.start()
            running.append(job)

        # wait for the first job to finish and handle it
        waitjob=running.pop(0)
        waitjob.join()
        if waitjob.status:
            error("Job failed, terminating")

        currsum -= waitjob.est_sz
        logger.debug("job %s finished, time %f, currsum is %d" % (waitjob, time.time()-waitjob.starttime, currsum))
        waitjob.finish(runstats)
        del waitjob

'''
This function is called to create the main tarball for a run, and also called recursively to archive each Unaligned/Project.
top: the directory to archive
arcdir: directory in which to create the tarball 
name: name of tarball in that directory

We'll also keep track of the files and bytes archived as we go.
Finally, to avoid a collision if there are multiple Project dirs with the same name, we are adding a counter to the tarball name.  
We will also create a log file and a finished file.  Thus:

141215_M01156_0172_000000000-AAR3L/
  141215_M01156_0172_000000000-AAR3L_0.tar
  141215_M01156_0172_000000000-AAR3L_1_Project_Ccc7.tar
  141215_M01156_0172_000000000-AAR3L_2_Project_Rdb9.tar
  141215_M01156_0172_000000000-AAR3L_archive.log
  141215_M01156_0172_000000000-AAR3L_finished.txt

'''
def makeTarball(top, arcdir, name, runstats):

    tfname="%s/%s.tar" % (arcdir, name % runstats.tarfiles)
    if os.path.exists(tfname) and not o.force: error("%s exists, use -f to force" % tfname)
    logger.debug("creating tarfile %s" % tfname) 
    if o.dryrun:
        tfp=None
    else:
        tfp=tarwrapper(tfname)

    filesToCheck=[]
    fastqs=[]
    for dirname, dirs, files in os.walk(top):
        files.sort()
        projects=prunedirs(dirname, dirs)
        for proj in projects:
            makeTarball(dirname+'/'+proj, arcdir, "%s_%%s_%s" % (o.runname, proj), runstats)
            
        fastqs+=handlefiles(dirname, files, tfp)

        # add the remaining keepers
        for f in files:
            fp=dirname+'/'+f
            try:
                sz=os.stat(fp).st_size
                logger.debug("adding %s (%d bytes)" % (fp, sz))
                runstats.files+=1; runstats.bytes+=sz
            except OSError:
                pass # don't panic on broken links

            if not o.dryrun: 
                tfp.add(fp)

    processJobs(fastqs, o.maxthds, runstats)
    runstats.tarfiles+=1

    if not o.dryrun and o.validate:
        tfp.validate()
    
'''
rundir: path to run, starting with ?.  E.g. 
arcdir: path to where tarballs should be created: 

'''
def archiveRun(rundir, arcdir):
    runstats=stats()

    o.runname=os.path.basename(rundir)
    starttime=time.time()

    arcdir=os.path.abspath(arcdir)
    o.finished='%s/%s_finished.txt' % (arcdir, o.runname)

    if os.path.exists(arcdir):
        if o.force: 
            logger.warning("%s exists, forcing overwrite" % o.finished)
        else:
            if os.path.exists(o.finished):
                logger.debug("%s appears finished, skipping" % arcdir)
                return runstats
            else:
                error("Partial archive of %s exists" % arcdir)
    else:
        logger.debug('makedirs %s' % arcdir)
        if not o.dryrun: os.makedirs(arcdir)

    # set up log file for this run 
    if not o.dryrun:
        h=logging.FileHandler('%s/%s_%s_archive.log' % (arcdir, o.runname, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
        h.setLevel(logging.DEBUG)
        h.setFormatter(formatter)
        logger.addHandler(h)
    
    logger.info("Archiving %s to %s" % (rundir, arcdir))
    if not os.path.isdir(rundir):
        error("Bad rundir %s" % rundir)

    o.tmpdir=os.path.abspath(o.tmpdir)
    if not os.path.isdir(o.tmpdir):
        error("Bad tmpdir %s" % o.tmpdir)

    # cd to dir above rundir
    os.chdir(rundir); os.chdir('..')

    makeTarball(o.runname, arcdir, "%s_%%s" % o.runname, runstats)

    if not o.dryrun: open(o.finished, 'w').close()
    t=time.time()-starttime
    bw=float(runstats.bytes)/(1024.0**2)/t
    logger.info("All Done %d Tarfiles, %d Files, %d quips, %f GB, %f Sec, %f MB/sec" % (runstats.tarfiles, runstats.files, runstats.quips, float(runstats.bytes)/1024**3, t, bw))
    if not o.dryrun: logger.removeHandler(h)
    return runstats

if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("--novalidate", dest="validate", action="store_false", default=True, help="don't validate")
    parser.add_argument("-p", "--projecttars", dest="projecttars", action="store_true", default=True, help="put projects into separate tars")
    parser.add_argument("-f", "--force", dest="force", action="store_true", default=False, help="force to overwrite tar or finished files")
    parser.add_argument("-t", "--tmpdir", dest="tmpdir", default="/tmp", help="where to create tmp files")
    parser.add_argument("-i", "--infile", dest="infile", help="file containing runs to archive")
    parser.add_argument("-r", "--rundir", dest="rundir", help="run directory")
    parser.add_argument("-a", "--arcdir", dest="arcdir", default="/SAY/archive/YCGA-729009-YCGA/archive", help="archve directory")
    parser.add_argument("--cuton", dest="cuton", help="date cuton; a run earlier than this 6 digit date will not be archived.  E.g. 150531.  Negative numbers are interpreted as days in the past, e.g. -45 means 45 days ago.")
    parser.add_argument("-c", "--cutoff", dest="cutoff", help="date cutoff; a run later than this will no be archived.  Similar to --cuton")
    parser.add_argument("-l", "--logfile", dest="logfile", default="archive", help="logfile prefix")
    parser.add_argument("--testlen", dest="testlen", type=int, default=10000, help="number of bytes to validate from each file")
    parser.add_argument("--maxthds", dest="maxthds", type=int, default=20, help="max threads")
    parser.add_argument("--maxsum", dest="maxsum", type=int, default=32000000000, help="max memory to use")

    o=parser.parse_args()
    starttime=time.time()

    # set up logging
    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf=logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose: hf.setLevel(logging.DEBUG)
    logger.addHandler(hf)

    # do some validation
    # require exactly one of -r, --automatic, -i
    if countTrue(o.rundir, o.automatic, o.infile) != 1:
        error("Must specify exactly one of -r --automatic -i")
    
    if o.automatic:
        if not o.cuton: o.cuton=-180
        if not o.cutoff: o.cutoff=-60

    if o.cuton: o.cuton=fixCut(o.cuton)
    if o.cuton: o.cutoff=fixCut(o.cutoff)

    if o.cuton and o.cutoff and o.cutoff < o.cuton:
        error("--cuton must be less than --cutoff")

    logger.debug("Invocation: " + " ".join(sys.argv))
    logger.debug("Cwd: " + os.getcwd())
    logger.debug("Options:" + str(o))

    totalstats=stats()
    runs=[]

    if o.rundir:
        runs=[o.rundir]

    elif o.infile:
        fp=open(o.infile)
        for l in fp:
            # denotes comments
            if l.startswith('#'): continue
            idx=l.find('#')
            if idx!=-1:
                l=l[:idx]

            runs.append(l.strip())

    elif o.automatic:
        rds=['/ycga-ba/ba_sequencers[12356]/sequencer?/runs/[0-9]*', '/ycga-gpfs/sequencers/illumina/sequencer*/runs/[0-9]*']
        runs=sorted(reduce(lambda a,b: a+b, [glob.glob(rd) for rd in rds]))

    passedruns=[]
    for run in runs:
        rundate=os.path.basename(run)[0:6]
        if o.cuton and rundate < o.cuton:
            logger.debug("Skipping %s: earlier than cuton" % run)
            continue
        if o.cutoff and rundate > o.cutoff:
            logger.debug("Skipping %s: later than cutoff" % run)
            continue
        passedruns.append(run)
    runs=passedruns

    # ok, here we go
    cwd=os.getcwd()
    logger.info("Going to archive %d runs" % len(runs))
    for run in runs:
        arcdir=mkarcdir(run, o.arcdir)
        runstats=archiveRun(run, arcdir)
        totalstats.comb(runstats)
        
        os.chdir(cwd) # archiveRun changed our dir, change it back now

    t=time.time()-starttime
    bw=float(totalstats.bytes)/(1024.0**2)/t

    logger.info("Archiving Finished %d Tarfiles, %d Files, %d quips, %f GB, %f Sec, %f MB/sec" % (totalstats.tarfiles, totalstats.files, totalstats.quips, float(totalstats.bytes)/1024**3, t, bw))

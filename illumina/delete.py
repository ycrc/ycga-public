
import glob, itertools, os, logging, argparse, time, shutil, subprocess

def fltr(r):
    rn=os.path.basename(r)
    try:
        dt=int(rn[:6])
        return dt < o.cutoff
    except:
        print ("weirdly named run: %s" % r)
        return False

runlocs=['/ycga-ba/ba_sequencers?/sequencer?/runs/*',
'/ycga-gpfs/sequencers/panfs/sequencers*/sequencer?/runs/*',
'/ycga-gpfs/sequencers/illumina/sequencer?/runs/*']

equiv=(
    ("/ycga-gpfs/sequencers/panfs/", "/SAY/archive/YCGA-729009-YCGA/archive/panfs/"),
    ("/ycga-gpfs/sequencers/panfs/sequencers1/", "/SAY/archive/YCGA-729009-YCGA/archive/panfs/sequencers/"),
    ("/ycga-ba/", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-ba/"),
    ("/ycga-gpfs/sequencers/illumina/", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-gpfs/sequencers/illumina/")
)

'''

runlocs=["/home/rob/project/tools/ycga-utils/illumina/FAKERUNS/sequencers/sequencer?/*",]

equiv=(
    ("/home/rob/project/tools/ycga-utils/illumina/FAKERUNS", "/home/rob/project/tools/ycga-utils/illumina/FAKEARCHIVE"),
)
'''

def chkArchive(r):
    for o, a in equiv:
        if r.startswith(o):
            arun=r.replace(o,a)
            chkfile=arun+'/'+os.path.basename(r)+"_finished.txt"
            st=os.path.exists(chkfile)
            if st:
                logger.debug("check %s %s ok" % (chkfile, st))
                return chkfile
    return None


if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-c", "--cutoff", dest="cutoff", type=int, required=True, help="cutoff date YYMMDD")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually delete")
    parser.add_argument("-l", "--logfile", dest="logfile", default="delete", help="logfile prefix")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")

    o=parser.parse_args()
    starttime=time.time()

    # set up logger
    logger = logging.getLogger('delete')
    formatter = logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc = logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose:
        hc.setLevel(logging.INFO)
        logger.addHandler(hc)

    hf = logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose:
        hf.setLevel(logging.DEBUG)
        logger.addHandler(hf)

    runs=itertools.chain.from_iterable([glob.glob(loc) for loc in runlocs])

    oldruns=[r for r in runs if fltr(r)]

    deletedcnt=0
    deletecnt=0
    missingcnt=0

    for r in oldruns:
        if r.endswith(".DELETED"):
            logger.info("Previously deleted %s" % (r,))
            deletedcnt+=1
            # already deleted
            continue
        a=chkArchive(r);
        if not a:
            logger.info("No archive for %s" % r)
            missingcnt+=1
            continue
        else:
            logger.info("Deleting %s" % (r,))
            deletecnt+=1
            if not o.dryrun:
                delfp=open(r+'.DELETED', 'w')
                delfp.write("Run deleted %s\n" % time.asctime())
                delfp.write("Archive is here: %s\n" % os.path.dirname(a))
                delfp.write("Files deleted:\n")
                delfp.flush()
                if subprocess.call(['find', r, '-ls'], stdout=delfp):
                    logger.error("Error doing find on %s" % r)
                delfp.close()
                shutil.rmtree(r)

    logger.info("All done.  Previous deleted %d, Archive missing %d, Deleted now %d runs." % (deletedcnt, missingcnt, deletecnt))

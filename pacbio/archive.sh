#pbarchive=/ycga-gpfs/project/fas/lsprog/tools/pacbioarchiver/test
pbarchive=/SAY/archive/YCGA-729009-YCGA/archive/pacbio

#pbdata=/ycga-ba/data1/pacbio/data
#pbdata=/ycga-gpfs/project/fas/lsprog/tools/pacbioarchiver/inputtest
#pbdata=/ycga-gpfs/sequencers/panfs/sequencers/pacbioA
pbdata=/ycga-gpfs/sequencers/pacbio/data

waitperiod=2592000 # 30 day wait period

logfile="archive_$(date +%Y%m%d_%H%M%S).log"

now=$(date +%s)

echo "Archiving Pacbio $(date) params: $*" | tee -a $logfile

IFS=$'\n'
for d in $(ls -1d ${pbdata}/*_[0-9]*)
do
    bd=$(basename "$d")
    filetime=$(stat -c %Y ${pbdata}/$bd)
    cutoff=$(expr $filetime + $waitperiod)
    if [ $now -lt $cutoff ]
    then
	echo "$bd too new, skipping" | tee -a  ${logfile}
    else
	tf=${pbarchive}/${bd}.tar
	ff=${pbarchive}/${bd}.finished

	if [ -f $tf ]
	then
	    if [ -f $ff ]
	    then
		echo "$ff $tf appears finished, skipping" | tee -a  ${logfile}

	    else
		echo "ERROR, $ff $tf appears partial, FIX!" | tee -a  ${logfile}
		exit 1
	    fi
	else
	    echo "*** archiving $bd $(date)" | tee -a  ${logfile}
	    if [ $# -eq 0 ] || [ "$1" != "-n" ]
	    then
		(cd $pbdata && tar cf $tf $bd && touch $ff) || exit 1
	    else
		echo "(cd $pbdata && tar cf $tf $bd && touch $ff) || exit 1"
            fi 
	    echo "*** done archiving $bd $(date)" | tee -a  ${logfile}
	fi
    fi
done

echo "All Done $(date)" | tee -a  ${logfile}

# Note: some tar testing shows that:
# tar czf -> 19m, 12GB
# tar cf -> 19s, 18GB
# tar cf - | gzip --fast,  8m34s, 13GB
# For now, I'd say it's not worth it to do either compression


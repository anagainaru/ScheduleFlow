#! /bin/bash

if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters"
    exit
fi

for i in $ScheduleFlow_PATH/draw/$1_*.tex; do echo "Compile $i ..."; pdflatex -output-directory $ScheduleFlow_PATH/draw $i > /dev/null; done
rm $ScheduleFlow_PATH/draw/$1_*.{aux,log,nav,out,snm,toc}
echo "Generating GIF ($1.gif) ..."
total_files=`ls -l $ScheduleFlow_PATH/draw/$1_*.pdf | wc -l`
magick -loop 1 -delay 200 -quality 100 -density 250 `for ((i=0; i<${total_files}; i++)); do echo "$ScheduleFlow_PATH/draw/$1_$i.pdf"; done` $ScheduleFlow_PATH/draw/$1.gif
echo  "Removing unecessary files ..."
rm $ScheduleFlow_PATH/draw/$1_*.pdf
if [ $2 == "delete" ]; then
	rm $ScheduleFlow_PATH/draw/$1_*.tex
fi
mv $ScheduleFlow_PATH/draw/$1.gif .

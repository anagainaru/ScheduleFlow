#! /bin/bash

if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters"
    exit
fi

for i in $SF_DRAW_PATH/draw/$1_*.tex; do echo "Compile $i ..."; pdflatex -output-directory $SF_DRAW_PATH/draw $i > /dev/null; done
rm $SF_DRAW_PATH/draw/$1_*.{aux,log,nav,out,snm,toc}
echo "Generating GIF ..."
total_files=`ls -l $SF_DRAW_PATH/draw/$1_*.pdf | wc -l`
convert -loop 1 -delay 200 -quality 100 -density 250 `for ((i=0; i<${total_files}; i++)); do echo "$SF_DRAW_PATH/draw/$1_$i.pdf"; done`$SF_DRAW_PATH/draw/$1.gif
echo  "Removing unecessary files ..."
rm $SF_DRAW_PATH/draw/$1_*.pdf
if [ $2 == "delete" ]; then
	rm $SF_DRAW_PATH/draw/$1_*.tex
fi

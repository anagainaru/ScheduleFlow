#! /bin/bash

if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters"
    exit
fi

for i in $SF_DRAW_PATH/$1_*.tex; do echo "Compile $i ..."; pdflatex $i > /dev/null; done
rm $1_*.{aux,log,nav,out,snm,toc}
echo "Generating GIF ..."
total_files=`ls -l $1_*.pdf | wc -l`
convert -loop 1 -delay 200 -quality 100 -density 250 `for ((i=0; i<${total_files}; i++)); do echo "$1_$i.pdf"; done` $SF_DRAW_PATH/$1.gif
echo  "Removing unecessary files ..."
rm $1_*.pdf
if [ $2 == "delete" ]; then
	rm $SF_DRAW_PATH/$1_*.tex
fi

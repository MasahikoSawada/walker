set terminal png
set output "/mnt/hgfs/Share/garbagemap/ghist.png"
set ytics nomirror
set y2tics
set grid
set xlabel "# of blocks"
set ylabel "# of dead tuples"
set y2label "% of total dead tuples"
set xrange[0:ymax]
set title table

plot infile using 1:2 with boxes title "# of dead tuples", \
     infile using 1:3 with lines title "% of total dead tuples" axes x1y2

set terminal png
set output "/mnt/hgfs/Share/garbagemap/".time."_".table."_hist.png"
set ytics nomirror
set grid
set xlabel "Range Number"
set ylabel "# of dead tuples"
set xrange[0:ymax]
set title "Histgram(".table.":".range.")"

plot infile with boxes title "# of dead tuples"

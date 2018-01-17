set terminal png
set output "/mnt/hgfs/Share/garbagemap/".table."_hist.png"
set ytics nomirror
set grid
set xlabel "Block number"
set ylabel "# of dead tuples"
set xrange[0:ymax]
set title "Histgram(".table.")"

plot infile with boxes title "# of dead tuples"

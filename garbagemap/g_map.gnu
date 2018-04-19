set terminal png
unset xtics
set output "/mnt/hgfs/Share/garbagemap/".table."_heatmap.png"
set title "Heat Map (".table.":".range.")"
set yrange [0:ymax] reverse
set ylabel "block #"
plot infile matrix with image

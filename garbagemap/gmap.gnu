set terminal png
unset xtics
set output "/mnt/hgfs/Share/garbagemap/gmap.png"
set title table
set yrange [0:ymax] reverse
set ylabel "block #"
plot infile matrix with image

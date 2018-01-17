DBNAME=${1:-postgres}
TABLE=${2:-hoge}
DATA="/tmp/data.csv"

PSQL="p master -d ${DBNAME} -X"

source ~/.bash_profile

# Prepare
${PSQL} -c "drop materialized view if exists tmp_mv"
${PSQL} -c "set garbagemap.summary_size to 1280; create materialized view tmp_mv as select * from gs_rank('${TABLE}')"

# Generate heat map
res=`${PSQL} -c "COPY (SELECT n_dead_tuples, n_dead_tuples FROM tmp_mv ORDER BY rangeno) TO '${DATA}' (NULL '0')"`
rows=`echo $res | sed 's/.*COPY \(.*\).*/\1/g'`
gnuplot -e "ymax=${rows};infile='${DATA}';table='${TABLE}'" g_map.gnu

# Generate garbage histogram
res=`${PSQL} -c "COPY (SELECT n_dead_tuples FROM tmp_mv ORDER BY rangeno) TO '${DATA}' (NULL '0')"`
rows=`echo $res | sed 's/.*COPY \(.*\).*/\1/g'`
gnuplot -e "ymax=${rows};infile='${DATA}';table='${TABLE}'" g_hist.gnu

# Generate garbage sorted histogram
res=`${PSQL} -c "COPY (SELECT rownum, n_dead_tuples, percent FROM tmp_mv ORDER BY n_dead_tuples DESC) TO '${DATA}' (NULL '0')"`
rows=`echo $res | sed 's/.*COPY \(.*\).*/\1/g'`
gnuplot -e "ymax=${rows};infile='${DATA}';table='${TABLE}'" g_sortedhist.gnu

${PSQL} -c "drop materialized view tmp_mv"

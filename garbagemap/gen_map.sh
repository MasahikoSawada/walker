DBNAME=${1:-postgres}
TABLE=${2:-hoge}
RANGE=${3:-1280} # 10MB
DATA="/tmp/data.csv"

PSQL="p master -d ${DBNAME} -X"

source ~/.bash_profile

# Prepare
${PSQL} -c "drop materialized view if exists tmp_mv"
${PSQL} -c "set garbagemap.summary_size to ${RANGE}; create materialized view tmp_mv as select * from gs_rank('${TABLE}')"

# Generate heat map
res=`${PSQL} -c "COPY (SELECT n_dead_tuples, n_dead_tuples FROM tmp_mv ORDER BY rangeno) TO '${DATA}' (NULL '0')"`
rows=`echo $res | sed 's/.*COPY \(.*\).*/\1/g'`
gnuplot -e "ymax=${rows};infile='${DATA}';table='${TABLE}';range=${RANGE}" g_map.gnu

# Generate garbage histogram
res=`${PSQL} -c "COPY (SELECT n_dead_tuples FROM tmp_mv ORDER BY rangeno) TO '${DATA}' (NULL '0')"`
rows=`echo $res | sed 's/.*COPY \(.*\).*/\1/g'`
gnuplot -e "ymax=${rows};infile='${DATA}';table='${TABLE}';range=${RANGE}" g_hist.gnu

# Generate garbage sorted histogram
res=`${PSQL} -c "COPY (SELECT rownum, n_dead_tuples, percent FROM tmp_mv ORDER BY n_dead_tuples DESC) TO '${DATA}' (NULL '0')"`
rows=`echo $res | sed 's/.*COPY \(.*\).*/\1/g'`
gnuplot -e "ymax=${rows};infile='${DATA}';table='${TABLE}';range=${RANGE}" g_sortedhist.gnu

${PSQL} -c "select count(n_all_visible), pg_relation_size('${TABLE}') / 8192, count(n_all_visible)::numeric(10,4) / (pg_relation_size('${TABLE}') /  8192) from tmp_mv where n_all_visible = 0"

${PSQL} -c "drop materialized view tmp_mv"

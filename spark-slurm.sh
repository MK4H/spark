#!/bin/bash
#SBATCH --time=0:15:00
#SBATCH --partition=small-hp
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=96G

# Run an example non-interactive Spark computation.
#
#   1. Image:"/mnt/home/_teaching/advpara/spark/spark" as img
#   2. Network adapter: "eno1" as dev
#   3. R/W directory: "${HOME}/spark/telsearch" as rwdir
#   4. Application: "/mnt/1/telsearch.py" as app
#
# Additionally, uses the following variables and values
#
#   1. Input file path: "/mnt/home/_teaching/advpara/spark/seznam.csv" as infilepath
#       This path can point anywhere in parlab filesystem
#       From this value, we parse filename as infilename and directory as infiledir,
#           which are then used during submit
#       The infiledir directory is mounted as /mnt/2 in all the containers
#       The infilename is used to build the /mnt/2/infilename path passed to the application through app_args
#
#   2. app_args contains arguments passed to our python script. Currently it is used to pass
#       the path to the input file and to the output directory, in which the output files are created
#
#
# Example:
#
#   $ sbatch spark-slurm.sh
#
# Spark configuration will be generated in ~/slurm-$SLURM_JOB_ID.spark; any
# configuration already there will be clobbered.

set -e

if [[ -z $SLURM_JOB_ID ]]; then
    echo "not running under Slurm" 1>&2
    exit 1
fi

img="/mnt/home/_teaching/advpara/spark/spark"
dev="eno1"
rwdir="${HOME}/spark/telsearch"
infilepath="/mnt/home/_teaching/advpara/spark/seznam.csv"
infilename="$(basename -- "${infilepath}")"
infiledir="$(dirname -- "${infilepath}")"
app="/mnt/1/telsearch.py"
app_args="/mnt/2/${infilename} /mnt/1/output-${SLURM_JOB_ID}"
conf=${HOME}/slurm-${SLURM_JOB_ID}.spark
TMP_DIR="/tmp/spark-${SLURM_JOB_ID}"

# What IP address to use for master?
if [[ -z $dev ]]; then
    echo "no high-speed network device specified"
    exit 1
fi
master_ip=$(  ip -o -f inet addr show dev "$dev" \
            | sed -r 's/^.+inet ([0-9.]+).+/\1/')
master_url=spark://${master_ip}:7077
if [[ -n $master_ip ]]; then
    echo "Spark master IP: ${master_ip}"
else
    echo "no IP address for ${dev} found"
    exit 1
fi

# Make Spark configuration
mkdir "$conf"
chmod 700 "$conf"
cat <<EOF > "${conf}/spark-env.sh"
SPARK_LOCAL_DIRS=${TMP_DIR}
SPARK_LOG_DIR=${TMP_DIR}/log
SPARK_WORKER_DIR=${TMP_DIR}
SPARK_LOCAL_IP=127.0.0.1
SPARK_MASTER_HOST=${master_ip}
PYTHONIOENCODING=utf8
EOF
mysecret=$(cat /dev/urandom | tr -dc '0-9a-f' | head -c 48)
cat <<EOF > "${conf}/spark-defaults.sh"
spark.authenticate true
spark.authenticate.secret $mysecret
EOF
chmod 600 "${conf}/spark-defaults.sh"

# Start the Spark master
ch-run -b "$conf" -b "$rwdir" -b "$infiledir" "$img" -- /opt/spark/sbin/start-master.sh
sleep 10
tail -7 ${TMP_DIR}/log/*master*.out
grep -Fq 'New state: ALIVE' ${TMP_DIR}/log/*master*.out

# Start the Spark workers
srun sh -c "   ch-run -b '${conf}' -b '$rwdir' -b '$infiledir' '${img}' -- \
                      /opt/spark/sbin/start-slave.sh ${master_url} \
            && sleep infinity" &
sleep 10
grep -F worker ${TMP_DIR}/log/*master*.out
tail -3 ${TMP_DIR}/log/*worker*.out

ch-run -b "$conf" -b "$rwdir" -b "$infiledir" "$img" -- \
       /opt/spark/bin/spark-submit --master "$master_url" \
       --executor-memory 64G \
       --driver-memory 32G \
       --executor-cores 64 \
       "$app" $app_args
# Let Slurm kill the workers and master

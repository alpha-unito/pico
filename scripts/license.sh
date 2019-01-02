# must be executed from the script directory
root=`pwd`/..

if [ ! -d licenseheaders ]; then
git clone git@github.com:johann-petrak/licenseheaders.git
fi
cd licenseheaders

for d in include tests examples; do
python licenseheaders.py -v -o "alpha group, CS department, University of Torino" -n pico -u "https://github.com/alpha-unito/pico" -y `date +"%Y"` -t lgpl-v3 -d $root/$d
done

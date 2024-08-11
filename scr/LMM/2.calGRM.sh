#!/bin/bash
#Set job requirements
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --partition=thin
#SBATCH --time=01:00:00


##The job is to make GRM and will use 24 cores to analyse it.
##Create a job with a for loop like:
#for part in {1..23}; do sbatch exampleJob.txt $part; done

#part=$1


##Creating directories to store the output data
#mkdir -p /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/mkGRM

##Going into the TMPDIR to run the analyse
cd /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/mkGRM

##define genofile path
genoFileFull=/gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed

##copy gcta
#cp "$HOME"/software/gcta/gcta-1.94.1-linux-kernel-3-x86_64/gcta64 ./

#copy genofile
#cp "$genoFileFull"/* ./
#for index in `seq 1 22`; do echo ukb_imp_pf_chr$index >> beds.txt; done


##Running gcta64, calculate dense grm
#./gcta64 --mbfile beds.txt --make-grm-part 10 "$part" --threads 20 --out temp

###part 2: merge together and set threshold. Run this part only when all results of part1 is finished
##merge GRM_temp file together
cat temp.part_10_*.grm.id > temp.grm.id
cat temp.part_10_*.grm.bin > temp.grm.bin
cat temp.part_10_*.grm.N.bin > temp.grm.N.bin

##set threshold to keep sparse grm
./gcta64 --grm temp --make-bK-sparse 0.05 --out sp_grm

##Copying the output to your home folder
cp ./*.log "$HOME"/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/output/LMM/grm
cp ./sp_grm* "$HOME"/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/output/LMM/grm

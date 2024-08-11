#!/bin/bash
#Set job requirements
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --partition=thin
#SBATCH --time=00:30:00

##The job takes one chromosome and will use 24 cores to analyse it.
##This job is to generate dosage from UKB imputed genotype


##Creating directories to store the output data
mkdir -p /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/LMM/output



##define genofile, phenofile, covarfile
phenoFileFull="$HOME/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/input/LMM/binary_pheno_lmm.txt"
phenoFile=$(basename -- "$phenoFileFull")
covarbFileFull="$HOME/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/input/LMM/binary_covarb_lmm.txt"
covarbFile=$(basename -- "$covarbFileFull")
covarcFileFull="$HOME/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/input/LMM/binary_covarc_lmm.txt"
covarcFile=$(basename -- "$covarcFileFull")
genoFileFull=/gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed

##Going into the TMPDIR to run the analyse
cd /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/LMM

##copy files to current folder
#copy pheno
cp "$phenoFileFull" ./
#copy binary covar
cp "$covarbFileFull" ./
#copy continuous covar
cp "$covarcFileFull" ./
#copy gcta
cp "$HOME"/software/gcta/gcta-1.94.1-linux-kernel-3-x86_64/gcta64 ./
#copy genofile
cp "$genoFileFull"/ukb_imp_pf_chr* ./
for index in `seq 1 22`; do echo ukb_imp_pf_chr$index >> beds.txt; done
#copy sparce GRM
cp  "$HOME"/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/output/LMM/grm/* ./

##print out all the files in the working directory to the log file. incase you need to troubleshoot 
#ls -lha >> ukb_gcta_lmm.log


# fastGWA mixed model (based on the sparse GRM generated above)
./gcta64 --mbfile beds.txt \
--grm-sparse sp_grm \
--fastGWA-mlm \
--pheno "$phenoFile" \
--covar "$covarbFile" \
--qcovar "$covarcFile" \
--threads 10 \
--maf 0.005 \
--out output/geno_assoc_lmm


## Another check incase something went wrong
#ls -lha >> ukb_gcta_lmm.log

##Copying the output to your home folder
cp ./*.log    "$HOME"/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/output/LMM/assoc
cp ./output/* "$HOME"/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/output/LMM/assoc

#!/bin/bash
#Set job requirements
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --partition=thin
#SBATCH --time=01:00:00

###This job is to extract genotype and perfrom QC
###The job takes 22 chromosome and will use 24 cores to analyse it.
##Create a job for every chromosome with a for loop like:
#for chromo in {1..23}; do sbatch exampleJob.txt $chromo; done

###Part1: make folders and assign variables

chromo=$1

phenoFileFull="$HOME/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/input/LMM/binary_pheno_lmm.txt"
phenoFile=$(basename -- "$phenoFileFull")
sampleFolder="/gpfs/work3/0/einf1831/Projects/UKB_Application64102/bulk_data/sample_files"


##Creating directories to store the output data
mkdir -p /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/chr"$chromo"
mkdir -p /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed
mkdir -p /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed_temp
mkdir -p /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/output

##Going into the folder to run the analyse
cd /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/chr"$chromo"

##Copying the pheno, covar and sample files.
cp $phenoFileFull ./
cp "$HOME"/project/phd_project_ukb/UKB_Application64102/CPSP_abdominal/input/LMM/control_eid_norel.txt ./
cp "$sampleFolder"/ukb*_imp_chr"$chromo"_*.sample ./
cp "$HOME"/software/plink/plink2_linux_x86_64_20220313/plink2 ./

##print out all the files in the working directory to the log file. In case you need to troubleshoot 
#ls -lha >> ukb_ls.log


###Part2: keep patients interested and prepare bed 
##Running plink2, adjust the parameters to your needs
./plink2 --bgen /gpfs/work4/0/ukbb_nij/Gdata_v3/bgen/ukb_imp_chr"$chromo"_v3.bgen ref-first \
--sample ukb*_imp_chr"$chromo"_*.sample \
--keep $phenoFile \
--snps-only just-acgt \
--mind 0.1 \
--geno 0.05 \
--extract /gpfs/work4/0/ukbb_nij/ukbb_nij/maf_info/ukb_mfi_ALL_v3_KeepMAF005andINFO8 \
--make-bed \
--out ../bed_temp/temp_ukb_imp_pf_chr"$chromo"


##Running plink2, test HWE in controls only
tempFolder=/gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed_temp

./plink2 --bfile "$tempFolder"/temp_ukb_imp_pf_chr"$chromo" \
--hwe 1e-6 \
--pheno ./binary_pheno_lmm.txt \
--keep control_eid_norel.txt \
--make-just-bim \
--out "$tempFolder"/chr"$chromo"variants_passhwe

##Running plink2, export bed
./plink2 --bfile "$tempFolder"/temp_ukb_imp_pf_chr"$chromo" \
--geno 0.05 \
--mind 0.1 \
--extract "$tempFolder"/chr"$chromo"variants_passhwe.bim \
--make-bed \
--out ../bed/ukb_imp_pf_chr"$chromo"



## Another check in case something went wrong
#ls -lha >> ukb_ls.log

##Copying the output to your home folder
#cp /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed_temp/*.log "$HOME"/phd_project_ukb/UKB_Application64102/CPSP_abdominal/LMM/log/makebgenlog/bed_temp/
#cp /gpfs/scratch1/shared/songli/CPSP_abdominal/LMM/input/bed/*.log "$HOME"/phd_project_ukb/UKB_Application64102/CPSP_abdominal/LMM/log/makebgenlog/bed/

##Remove the rm command if you want to access the used files after analyses. Files on scratch are automatically removed in ~7 days 




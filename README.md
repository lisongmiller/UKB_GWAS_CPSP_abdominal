# UKB_GWAS_CPSP_abdominal
script for GWAS on CPSP after abdominal surgeries in the UK Biobank
In the scr folder, there are three scripts: 

1.define_CPSP_abd.py: define chronic postsurgical pain based on operation and prescription data.
2.add_operation_category.py: assign surgery category based on operation codes chapters.
3.prepare_gwas_phenotype.py: prepare phenotype files for GWAS analysis.

In the input folder, there are two subfolders:

1.analgesic_code: stores analgesics coded in different code systems in the UK Biobank primary care data.
2.operation_code: stores operation codes in OPCS4 or read v2 or read v3 code.

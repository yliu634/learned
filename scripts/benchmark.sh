touch results/benchmark_result.json
rm results/results.json
touch results/results.json
rm results/log_stats.out
touch results/log_stats.out

######## LUDO HASHING ##########################
bash scripts/run.sh > results/log_stats.out



if false; then
######## LEARNED LINEAR PROBING ################

for bucket_size in 1
do
    #54 82 122 185 300
    for overalloc in 34 
    do
        for model_name in "RMIHash" #"RadixSplineHash" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 100 1024 > results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 100 1024 > results/log_stats.out
            #python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 100 1024
            bash scripts/run.sh >> results/log_stats.out
            #cat results/benchmark_result.json >>results/results.json
        done
    done
done


######## TRADITIONAL LINEAR PROBING ################

for bucket_size in 1
do
    for overalloc in 34 54 82 122 185 300 
    do
        for model_name in "MURMUR" "MultPrime64" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Linear" "Balanced" 0 0 0 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Linear" "Balanced" 0 0 0 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Linear" "Balanced" 0 0 0
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done


######## PERFECT LINEAR PROBING ################

for bucket_size in 1
do
    for overalloc in 34 54 82 122 185 300 
    do
        for model_name in "MWHC" "BitMWHC" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done

######## TRADITIONAL CHAINED PROBING ################

for bucket_size in 1 
do
    for overalloc in 10050 10067 10080 0 34 100 300 
    do
        for model_name in "MURMUR" "MultPrime64" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0  >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done

######## LEARNED CHAINED PROBING ################

for bucket_size in 1 
do
    for overalloc in 10050 10067 10080 0 34 100 300
    do
        for model_name in "RMIHash" "RadixSplineHash" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 100 1024 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 100 1024 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 100 1024
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done


####### PERFECT CHAINED PROBING ################

for bucket_size in 1
do
    for overalloc in 34 54 82 122 185 300 
    do
        for model_name in "MWHC" "BitMWHC" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done


######## TRADITIONAL CUCKOO ################
for bucket_size in 4 
do
    for overalloc in 34 25 17 11 5
    do
        for model_name in "MURMUR" "MultPrime64"  
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Biased" 5 0 0 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Biased" 5 0 0 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Biased" 5 0 0
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done

######## LEARNED CUCKOO ################
for bucket_size in 4 
do
    for overalloc in 34 25 17 11 5
    do
        for model_name in "RMIHash" "RadixSplineHash" 
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 100 1024 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 100 1024 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 100000 32
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done

######## PERFECT CUCKOO ################

for bucket_size in 4 
do
    for overalloc in 34 25 17 11 5
    do
        for model_name in "BitMWHC" "MWHC"  
        do
            echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Cuckoo" "Biased" 5 0 0 >>results/results.json
            echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Cuckoo" "Biased" 5 0 0 >>results/log_stats.out
            python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Cuckoo" "Biased" 5 0 0
            bash scripts/run.sh >>results/log_stats.out
            cat results/benchmark_result.json >>results/results.json
        done
    done
done

fi


rm results/benchmark_result.json



####################################################### CODE DUMP ############################################################################
####################################################### CODE DUMP ############################################################################
####################################################### CODE DUMP ############################################################################
####################################################### CODE DUMP ############################################################################

# bucket_size=int(sys.argv[1])
# overalloc=int(sys.argv[2])
# model_name=str(sys.argv[3])
# model_type=str(sys.argv[4])
# hashing_scheme=str(sys.argv[5])
# kickinit_strat=str(sys.argv[6])
# kickinit_strat_bias=int(sys.argv[7])
# max_models=int(sys.argv[8])
# max_error=int(sys.argv[9])

# "MURMUR":"using MURMUR = hashing::MurmurFinalizer<Key>;",
#     "MultPrime64":"using MultPrime64 = hashing::MultPrime64;",
#     "FibonacciPrime64":"using FibonacciPrime64 = hashing::FibonacciPrime64;",
#     "AquaHash":"using AquaHash = hashing::AquaHash<Key>;",
#     "XXHash3":"using XXHash3 = hashing::XXHash3<Key>;",
#     "MWHC":"using MWHC = exotic_hashing::MWHC<Key>;",
#     "FST":"using FST = exotic_hashing::FastSuccinctTrie<Data>;",
#     "RadixSplineHash":"using RadixSplineHash = learned_hashing::RadixSplineHash<std::uint64_t,max_error,max_models>;",
#     "RMIHash":"using RMIHash = learned_hashing::RMIHash<std::uint64_t,max_models>;"
# python3 tools/edit_benchmark.py 1 20 "MWHC" "Exotic" "Chained" "Biased" 0 0 0

# python3 tools/edit_benchmark.py 1 20 "BitMWHC" "Exotic" "Chained" "Balanced" 0 0 0

#  python3 tools/edit_benchmark.py 1 100 "RMIHash" "Model" "Chained" "Balanced" 0 100 1024 

# python3 tools/edit_benchmark.py 4 15 "MWHC" "Exotic" "Cuckoo" "Biased" 5 0 0

# python3 tools/edit_benchmark.py 1 50 "MURMUR" "Traditional" "Linear" "Balanced" 0 0 0
# python3 tools/edit_benchmark.py 1 34 "RMIHash" "Traditional" "Linear" "Balanced" 0 100 1024
# python3 tools/edit_benchmark.py 4 20 RMIHash "Model" "Cuckoo" "Biased" 5 1000 1024

# touch results/benchmark_result.json

## HASH Computation

# for bucket_size in 1 
# do
#     for overalloc in  100 
#     do
#         for model_name in "MURMUR" "MultPrime64" "XXHash3" "AquaHash" "FibonacciPrime64"
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0  >>hash_comp_stats.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0
#             bash scripts/run.sh >>hash_comp_stats.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# for bucket_size in 1 
# do
#     for num_models in 1 10 100 1000 10000 100000 1000000 10000000
#     do
#         for model_name in  "RMIHash" 
#         do
#             echo "Start Here" $bucket_size 100 $model_name "Model" "Chained" "Balanced" 0 $num_models 10024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size 100 $model_name "Model" "Chained" "Balanced" 0 $num_models 10024 >>hash_comp_stats.out
#             python3 tools/edit_benchmark.py $bucket_size 100 $model_name "Model" "Chained" "Balanced" 0 $num_models 10024
#             bash scripts/run.sh >>hash_comp_stats.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# for bucket_size in 1 
# do
#     for max_error in 1 2 5 10 25 100 200 500 1000 10000 100000
#     do
#         for model_name in  "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size 100 $model_name "Model" "Chained" "Balanced" 0 1 $max_error >>kapil_results/results.json
#             echo "Start Here" $bucket_size 100 $model_name "Model" "Chained" "Balanced" 0 1 $max_error >>hash_comp_stats.out
#             python3 tools/edit_benchmark.py $bucket_size 100 $model_name "Model" "Chained" "Balanced" 0 1 $max_error
#             bash scripts/run.sh >>hash_comp_stats.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done


# for bucket_size in 1
# do
#     for overalloc in 10
#     do
#         for model_name in "MWHC" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Chained" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Chained" "Balanced" 0 0 0 >>hash_comp_stats.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Chained" "Balanced" 0 0 0
#             bash scripts/run.sh >>hash_comp_stats.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done




# ###########################LINEAR############################
# ###########################LINEAR############################
# ###########################LINEAR############################


# #Model Linear Experiments 

# # python3 tools/edit_benchmark.py 1 50 RMIHash Model Linear Balanced 0 1000 1024
#load factor 75,66,55,45,35,25
# for bucket_size in 1 
# do
#     for overalloc in 34 54 82 122 185 300 
#     do
#         for model_name in "RMIHash"  "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 1 10024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 1 10024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 1 10024
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# for bucket_size in 1
# do
#     for overalloc in 34 54 82 122 185 300
#     do
#         for model_name in "RMIHash" "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 100 1024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 100 1024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Linear" "Balanced" 0 100 1024
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done


# #Traditional Linear Experiments 

# for bucket_size in 1
# do
#     for overalloc in 34 54 82 122 185 300 
#     do
#         for model_name in "MURMUR" "MultPrime64" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Linear" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Linear" "Balanced" 0 0 0 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Linear" "Balanced" 0 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# #Perfect Hashing Linear Experiments 

# for bucket_size in 1
# do
#     for overalloc in 34 54 82 122 185 300 
#     do
#         for model_name in "MWHC" "BitMWHC" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Linear" "Balanced" 0 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done




###########################CHAINED############################
###########################CHAINED############################
###########################CHAINED############################
#Traditional Chained Experiments 
#load factor 200,150,125,100,75,50,25
# for bucket_size in 1 
# do
#     for overalloc in 10050 10067 10080 0 34 100 300 
#     do
#         for model_name in "MURMUR" "MultPrime64" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0  >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Chained" "Balanced" 0 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done


# #Model Chained Experiments 

# for bucket_size in 1 
# do
#     for overalloc in 10050 10067 10080 0 34 100 300
#     do
#         for model_name in  "RMIHash" "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 1 10024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 1 10024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 1 10024
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# for bucket_size in 1 
# do
#     for overalloc in 10050 10067 10080 0 34 100 300
#     do
#         for model_name in "RMIHash" "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 100 1024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 100 1024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Chained" "Balanced" 0 100 1024
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done


# #Exotic Chained Experiments 

# for bucket_size in 1
# do
#     for overalloc in 10050 10067 10080 0 34 100 300
#     do
#         for model_name in "MWHC" "BitMWHC"
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Chained" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Chained" "Balanced" 0 0 0 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Chained" "Balanced" 0 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done






###########################Cuckoo############################
###########################Cuckoo############################
###########################Cuckoo############################



#Model Cuckoo Experiments 

# for bucket_size in 4 8
# do
#     for overalloc in 15 30 
#     do
#         for model_name in "RMIHash"  "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Balanced" 0 1000 1024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Balanced" 0 1000 1024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Cuckoo" "Balanced" 0 1000 1024
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# for bucket_size in 4 8
# do
#     for overalloc in 15 30
#     do
#         for model_name in "RMIHash" "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Balanced" 0 100000 32 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Balanced" 0 100000 32 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Cuckoo" "Balanced" 0 100000 32
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

#Traditional Cuckoo Experiments 


# for bucket_size in 4 8
# do
#     for overalloc in 15 30
#     do
#         for model_name in "MURMUR" "MultPrime64"  "XXHash3"
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Balanced" 0 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Balanced" 0 0 0 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Balanced" 0 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

#load factor 75,80,85,90,95

#Model Cuckoo Experiments 

# for bucket_size in 4 
# do
#     for overalloc in 34 25 17 11 5
#     do
#         for model_name in "RMIHash"  "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 1 10024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 1 10024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 1000 1024
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

# for bucket_size in 4 
# do
#     for overalloc in 34 25 17 11 5
#     do
#         for model_name in "RMIHash" "RadixSplineHash" 
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 100 1024 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 100 1024 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Model" "Cuckoo" "Biased" 5 100000 32
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

#Traditional Cuckoo Experiments 


# for bucket_size in 4 
# do
#     for overalloc in 34 25 17 11 5
#     do
#         for model_name in "MURMUR" "MultPrime64"  
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Biased" 5 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Biased" 5 0 0 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Traditional" "Cuckoo" "Biased" 5 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done

#Exotic Cuckoo Experiments 

# for bucket_size in 4 
# do
#     for overalloc in 34 25 17 11 5
#     do
#         for model_name in "BitMWHC" "MWHC"  
#         do
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Cuckoo" "Biased" 5 0 0 >>kapil_results/results.json
#             echo "Start Here" $bucket_size $overalloc $model_name "Exotic" "Cuckoo" "Biased" 5 0 0 >>data_stats_mar14.out
#             python3 tools/edit_benchmark.py $bucket_size $overalloc $model_name "Exotic" "Cuckoo" "Biased" 5 0 0
#             bash scripts/run.sh >>data_stats_mar14.out
#             cat results/benchmark_result.json >>kapil_results/results.json
#         done
#     done
# done



# rm results/benchmark_result.json


import sys

# hashing functions list
hash_mapping_dict={
    "MURMUR":"using MURMUR = hashing::MurmurFinalizer<Key>;",
    "MultPrime64":"using MultPrime64 = hashing::MultPrime64;",
    "FibonacciPrime64":"using FibonacciPrime64 = hashing::FibonacciPrime64;",
    "AquaHash":"using AquaHash = hashing::AquaHash<Key>;",
    "XXHash3":"using XXHash3 = hashing::XXHash3<Key>;",
    "MWHC":"using MWHC = exotic_hashing::MWHC<Key>;",
    "BitMWHC":"using BitMWHC = exotic_hashing::BitMWHC<Key>;",
    "FST":"using FST = exotic_hashing::FastSuccinctTrie<Data>;",
    "RadixSplineHash":"using RadixSplineHash = learned_hashing::RadixSplineHash<std::uint64_t,num_radix_bits,max_error,100000000>;",
    "RMIHash":"using RMIHash = learned_hashing::RMIHash<std::uint64_t,max_models>;"
}

# traditional, with mchine learning model or mph
model_type_dict={
    "Traditional":"",
    "Exotic":"Exotic",
    "Model":"Model"

}
# hashing schemes
scheme_dict={
    "Cuckoo":"Cuckoo",
    "Linear":"Linear",
    "Chained":"Chained"
}

kickin_strat_dict={
    "Balanced":"using KickingStrat = kapilmodelhashtable::KapilModelBalancedKicking;",
    "Biased":"using KickingStrat = kapilmodelhashtable::KapilModelBiasedKicking<kickinit_strat_bias>;"
}


bucket_size=int(sys.argv[1])
overalloc=int(sys.argv[2])
model_name=str(sys.argv[3])
model_type=str(sys.argv[4])
hashing_scheme=str(sys.argv[5])
kickinit_strat=str(sys.argv[6])
kickinit_strat_bias=int(sys.argv[7])
max_models=int(sys.argv[8])
max_error=int(sys.argv[9])
num_radix_bits=18
namespace_str="_"

def hash_line(model_name,model_type,max_models,max_error,num_radix_bits):
    if model_type_dict[model_type]!="Model":
        return hash_mapping_dict[model_name]
    ans_str=hash_mapping_dict[model_name]

    if "max_models" in ans_str:
        ans_str=ans_str.replace("max_models",str(max_models))

    if "max_error" in ans_str:
        ans_str=ans_str.replace("max_error",str(max_error)) 

    if "num_radix_bits" in ans_str:
        ans_str=ans_str.replace("num_radix_bits",str(num_radix_bits))        

    return ans_str    

def kick_line(hashing_scheme,kickinit_strat,kickinit_strat_bias):

    if scheme_dict[hashing_scheme]!="Cuckoo":
        return ""

    ans_str=kickin_strat_dict[kickinit_strat]

    if  "kickinit_strat_bias" in ans_str:
        ans_str=ans_str.replace("kickinit_strat_bias",str(kickinit_strat_bias))

    return ans_str    

def bench_line(hashing_scheme,model_type,bucket_size,overalloc):

    ans_str="BenchmarKapil"+scheme_dict[hashing_scheme]+model_type_dict[model_type]
    ans_str+="("+str(bucket_size)+","+str(overalloc)+","+model_name

    if scheme_dict[hashing_scheme]=="Cuckoo":
        ans_str+=",KickingStrat);"
    else:
        ans_str+=");"

    return ans_str    



file1=open("src/benchmarks_backup.cpp","r")
file2=open("src/benchmarks.cpp","w")

# Iterate over each line in the file
for line in file1.readlines():
    file2.write(line)

# Release used resources
file1.close()
file2.close()


# print(max_models,"max_models")

with open("src/benchmarks.cpp", "a") as myfile:
    
    write_str="using namespace "+namespace_str+";\n\n"
    write_str+=hash_line(model_name,model_type,max_models,max_error,num_radix_bits)+"\n"
    write_str+=kick_line(hashing_scheme,kickinit_strat,kickinit_strat_bias)+"\n"
    write_str+=bench_line(hashing_scheme,model_type,bucket_size,overalloc)

    write_str+="\n\n\n\nBENCHMARK_MAIN();\n"
    #write_str+="std::cout << \"Benchmarking complete!\" << std::endl;\n"

    myfile.write(write_str)

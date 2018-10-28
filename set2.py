# Gilat Mandelbaum Set2

from nltk.tokenize import word_tokenize
import numpy as np
import nltk
#nltk.download()

def data_stream():
    """Stream the data in 'leipzig100k.txt' """
    with open('leipzig100k.txt', 'r') as f:
        for line in f:
            for w in word_tokenize(line):
                if w.isalnum():
                    yield w
   
def bloom_filter_set():
    """Stream the data in 'Proper.txt' """
    with open('Proper.txt', 'r') as f:
        for line in f:
            yield line.strip()



############### DO NOT MODIFY ABOVE THIS LINE #################


# Implement a universal hash family of functions below: each function from the
# family should be able to hash a word from the data stream to a number in the
# appropriate range needed.

def findPrime(n):
    """Returns a prime number larger than n
    """
    def isPrime(k):
        import math
        for divisor in range(2, round(math.sqrt(n)-0.5)):
            if k%divisor==0:
                return False
        return True
    if n%2==0:
        candidate = n+1
    else:
        candidate = n
    while not isPrime(candidate):
        candidate += 2
    return candidate  


def uhf(rng):
    """Returns a hash function that can map a word to a number in the range
    0 - rng
    """
    p = findPrime(rng)
    m = 2*rng
    a = np.random.randint(1,p)
    b = np.random.randint(0,p)
    return lambda x: ((a*x+b)%p)%m


############### 

################### Part 1 ######################

from bitarray import bitarray
size = 2**18   # size of the filter

hash_fns = [None, None, None, None, None]  # place holder for hash functions
bloom_filter = bitarray(size)
num_words = 0         # number in data stream
num_words_in_set = 0  # number in Bloom filter's set

# Create 5 hash functions
for i in range(len(hash_fns)):
    hash_fns[i] = uhf((size-1)/5)

#for word in bloom_filter_set(): # add the word to the filter by hashing etc.
#    pass
# Create dictionary that holds all words in data set as keys, and values as list of positions declared by the hash functions
dictionary = dict()

# Loop through bloom_filter_set
for word in bloom_filter_set():
    num_words_in_set += 1 # count number of words
    dictionary[word] = []
    for h in hash_fns:
        x = int(h(num_words_in_set) % size) # utilize hash functions
        dictionary[word].append(x) # save position in dictionary list
        bloom_filter[x] = True #add to bloom filter array

# Create another dictionary to collect words that are not in the bloom_filter_set dictionary, along with list of their positions declared by the hash functions
dict_stream = dict()

# Loop through data stream 
for word in data_stream(): 
    num_words += 1 # count number of words
    if word not in dictionary.keys(): # If word not in dictionary, add to data stream's dictionary
        dict_stream[word]=[]
        for h in hash_fns:
            y = int(h(num_words) % size)
            dict_stream[word].append(y)
    else:
        pass

# Count number of faLse positives
fp = 0 # False positives counter
for key in dict_stream.keys():
    count_p = 0 # Counts number of bloom filter positions where the position is True
    for i in dict_stream[key]:
        if bloom_filter[i] == True:
            count_p += 1
        else:
            pass
        if count_p == 5: # If all 5 positions were taken place, then counts as false positive
            fp += 1
        else:
            pass
            
print('False Positives count:',fp)
print('Total number of words in stream = %s'%(num_words,))
print('Total number of words in stream = %s'%(num_words_in_set,))
     
################### Part 2 ######################

hash_range = 24 # number of bits in the range of the hash functions
fm_hash_functions = [None]*35  # Create the appropriate hashes here

# Create 35 hash functions
for i in range(len(fm_hash_functions)):
    fm_hash_functions[i] = uhf(hash_range)
# Split to 7 groups
fm_hash_functions1 = fm_hash_functions[:5]
fm_hash_functions2 = fm_hash_functions[5:10]
fm_hash_functions3 = fm_hash_functions[10:15]
fm_hash_functions4 = fm_hash_functions[15:20]
fm_hash_functions5 = fm_hash_functions[20:25]
fm_hash_functions6 = fm_hash_functions[25:30]
fm_hash_functions7 = fm_hash_functions[30:]

# Create list of all hash function groups; easier to loop through.
all_hash = [fm_hash_functions1,fm_hash_functions2,fm_hash_functions3,fm_hash_functions4,fm_hash_functions5,fm_hash_functions6,fm_hash_functions7]


def num_trailing_bits(n):
    """Returns the number of trailing zeros in bin(n)

    n: integer
    """
    count = 0
    value = str(n) # Convert numeric bits into string
    for i in range(1,len(value)): # Check for trailing 0s, breaks if not 0
        if value[-i] == '0':
            count+=1
        else:
            break
    
    if count == len(value)-1: # if count is equal to the length of the string, that means all values are 0, but count will equal 0
        count = 0
    return cwordount


num_word_fm = 0
distinct_average = [] # Holds the average number of distinct words for each group
for h in all_hash:
    distinct_per_group = [] # Holds the number of distinct words per function in a group
    for funct in h:
        zeroes = [] # Holds the number of trailing zeroes per word in the hash function
        for word in data_stream(): # Use hash function as previous part
            num_word_fm += 1
            x = (funct(num_word_fm) % hash_range) 
            num = format(x, '024b') # Format word into bits
            zeroes.append(num_trailing_bits(num)) # use num_trailing_bits function
        distinct_per_group.append(2**max(zeroes)) # Find number of distinct groups
    distinct_average.append(sum(distinct_per_group)/5) # Find average number of distinct words per group
            
distinct_average.sort() # Sort averages in numerical order
num_distinct = distinct_average[3]   # postion 3 shows the median. 
print("Estimate of number of distinct elements = %s"%(num_distinct,))

################### Part 3 ######################

var_reservoir = [0]*512
second_moment = 0
third_moment = 0

# You can use numpy.random's API for maintaining the reservoir of variables
# Create list of random indicies from the length of data_stream
import random
words = list(range(2059555))
random.shuffle(words)
var_reservoir = sorted(words[:len(var_reservoir)]) # Add random indicies to reservoir

#for word in data_stream(): # Imoplement the AMS algorithm here
#    pass   
variables = {} # Dictionary to hold the words in positions as keys and frequency as values
count = 0
# Loop through data stream, if that word index is in var_reservoir, start counting how many times that word appears.
for word in data_stream():
    count += 1
    if count in var_reservoir and word not in variables:
        variables[word] = 0
    if word in variables:
        variables[word] += 1
        
#  Equation for second moment: takes sum of each key's value
second_moment = int((len(variables)) * sum((2 * v - 1) for v in variables.values())) 
# Equation for third moment comes from second moment where 2v-1 = v**2-(v-1)**2. For 3: v**3-(v-1)**3
third_moment =  int((len(variables)) * sum((3 * v**2 - 3*v + 1) for v in variables.values())) # Equation for third moment

      
print("Estimate of second moment = %s"%(second_moment,))
print("Estimate of third moment = %s"%(third_moment,))

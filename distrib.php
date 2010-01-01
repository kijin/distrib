<?php

/**
 * DISTRIB : A Consistent Hashing Library.
 * 
 * This class can be used by client libraries for key-value stores to distribute
 * keys across multiple backends. Several algorithms are supported, from very
 * simple naive hashing to redundant consistent hashing.
 * 
 * Copyright (c) 2010, Kijin Sung <kijinbear@gmail.com>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

class Distrib
{
    /**
     * Private properties, no trespassing.
     */
    
    private $algorithm = '';
    
    private $backends = array();
    private $backends_count = 0;
    
    private $hashring = array();
    private $hashring_count = 0;
    
    private $replicas = 0;
    private $slices_count = 0;
    private $slices_div = 0;
    
    private $cache = array();
    private $cache_count = 0;
    private $cache_max = 0;
    
    
    /**
     * Constructor.
     * 
     * The algorithm must be one of 'naive', 'consistent', 'redundant'.
     * The list of backends must be an array, keys being backend IDs and values
     * being their respective weights.
     * 
     * @param  string  The name of the algorithm to use.
     * @param  array   An array of backends and their respective weights.
     * @param  int     The number of replicas per backend. Default: 256.
     * @param  int     The number of cache slots. Default: 256.
     */
    
    public function __construct($algorithm, $backends, $replicas = 256, $cache_max = 256)
    {
        // Keep the data.
        
        $this->algorithm = $algorithm;
        $this->backends = $backends;
        $this->backends_count = count($backends);
        $this->replicas = ($replicas ? $replicas : 256) * $this->backends_count;
        $this->cache_max = $cache_max;
        
        // Some sanity checks.
        
        if (!in_array($algorithm, array('naive', 'consistent', 'redundant')))
        {
            throw new Exception('Unrecognized key distribution algorithm: ' . $algorithm);
        }
        
        if (!$this->backends_count)
        {
            throw new Exception('No backends to distribute keys against!');
        }
        
        // Create the hashring for the naive algorithm.
        
        if ($this->algorithm === 'naive')
        {
            // Initialize the hashring and the count.
            
            $this->hashring = array();
            $this->hashring_count = 0;
            
            // Iterate the backends.
            
            foreach ($this->backends as $backend => $weight)
            {
                // Add to the hashring count.
                
                $this->hashring_count += $weight;
                
                // Create as many replicas as $weight.
                
                for ($i = 0; $i < $weight; $i++)
                {
                    $this->hashring[] = $backend;
                }
            }
        }
        
        // Create the hashring for the [redundant] consistent hashing algorithm.
        
        else
        {
            // How many slices do we want?
            
            $this->slices_count = $this->replicas / 8;
            $this->slices_half = $this->slices_count / 2;
            $this->slices_div = (2147483648 / $this->slices_half);
            
            // Initialize the hashring.
            
            $this->hashring = array_fill(0, $this->slices_count, array());
            
            // Calculate a multiplier, for a total of a fixed number of replicas.
            
            $multiplier = $this->replicas / array_sum($this->backends);
            
            // Interate the backends.
            
            foreach ($this->backends as $backend => $weight)
            {
                // Adjust the weight with the multiplier.
                
                $weight = round($weight * $multiplier);
                
                // Create as many replicas as $weight.
                
                for ($i = 0; $i < $weight; $i++)
                {
                    $position = crc32($backend . ':' . $i);
                    $slice = floor($position / $this->slices_div) + $this->slices_half;
                    $this->hashring[$slice][$position] = $backend;
                }
            }
            
            // Sort each slice of the hashring.
            
            for ($i = 0; $i < $this->slices_count; $i++)
            {
                if ($this->hashring[$i] === null) echo $i . "\n";
                ksort($this->hashring[$i], SORT_NUMERIC);
            }
        }
    }
    
    
    /**
     * MAP method.
     * 
     * Call this method to find out which backend a key belongs to. If you use
     * redundant consistent hashing, you may also supply an integer argument
     * to find several backends.
     * 
     * @param   string  The key to map.
     * @param   int     The number of backends to return. [optional: only with redundant consistent hashing]
     * @return  array   An array of backend IDs.
     */
    
    public function map($key, $count = 1)
    {
        // If we have only one backend, return it.
        
        if ($this->backends_count == 1) return array_keys($this->backends);
        
        // If the key has already been mapped, return the cached entry.
        
        if ($this->cache_max > 0 && isset($this->cache[$key])) return $this->cache[$key];
        
        // If $count is greater than or equal to the number of available backends, return all.
        
        if ($count >= $this->backends_count) return array_keys($this->backends);
        
        // Initialize the return array.
        
        $return = array();
        
        // Do the mapping.
        
        switch ($this->algorithm)
        {
            // Naive distribution.
            
            case 'naive':
                
                // Very basic CRC32 + modulus.
                
                $position = abs(crc32($key)) % $this->hashring_count;
                $return = array($this->hashring[$position]);
                break;
            
            // Single consistent hashing.
            
            case 'consistent':
                
                // Just use the redundant algorithm, with $count = 1.
                
                $count = 1;
                
            // Redundant consistent hashing.
                
            case 'redundant':
                
                // Get the key's CRC32.
                
                $crc32 = crc32($key);
                
                // Select the slice to begin with.
                
                $slice = floor($crc32 / $this->slices_div) + $this->slices_half;
                
                // This counter prevents going through more than 1 loop.
                
                $looped = false;
                
                // Search the hashring.
                
                while (true)
                {
                    // Interate over one slice at a time.
                    
                    foreach ($this->hashring[$slice] as $position => $backend)
                    {
                        // If we have a usable backend, add to the return array.
                        
                        if ($position >= $crc32)
                        {
                            // If $count = 1, no more checks are necessary.
                            
                            if ($count === 1)
                            {
                                $return = array($backend);
                                break 3;
                            }
                            
                            // Otherwise, check for and skip duplicates.
                            
                            elseif (!in_array($backend, $return))
                            {
                                $return[] = $backend;
                                if (count($return) >= $count) break 3;
                            }
                        }
                    }
                    
                    // Continue to the next slice.
                    
                    $slice++;
                    
                    // If at the end of the hashring.
                    
                    if ($slice >= $this->slices_count)
                    {
                        // If already looped once, something is wrong.
                        
                        if ($looped) break 2;
                        
                        // Otherwise, loop back to the beginning.
                        
                        $crc32 = -2147483648;
                        $slice = 0;
                        $looped = true;
                    }
                }
            
            // If we're here, something is wrong.
            
            default: $return = array();
        }
        
        // Cache the result for quick retrieval in the future.
        
        if ($this->cache_max > 0)
        {
            // Add to internal cache.
            
            $this->cache[$key] = $return;
            $this->cache_count++;
            
            // If the cache is getting too big, clear it.
            
            if ($this->cache_count > $this->cache_max)
            {
                $this->cache = array();
                $this->cache_count = 0;
            }
        }
        
        // Return the result.
        
        return $return;
    }
}

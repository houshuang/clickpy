This repository contains scripts used to analyze Coursera clicklogs. It's basically two stages, log2h5, which is fairly well tested, parses the data (including extracting query arguments from the URLs) and imports it into HDF5, with manual memoization. Then we have scripts that further process the data, for example by compressing all video events for a given video into one event with tags - this will obviously be very different depending on how you intend to analyze the data. Anyway, feel free to look at the code, and use any parts of it that are useful to you.

There is some writeup of the approach here: http://reganmian.net/blog/2014/03/10/parsing-massive-clicklogs/

Released under an MIT license. (c) Stian Håklev, 2015

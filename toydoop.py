"""
Python map-reduce implementation for testing and
proof-of-concept experiments.

Use it like this:

    # first define your mapper and reducer functions
    # they should be generators i.e. use "yield" and
    # not "return"
    def mapper(key,val):
        # can yield multiple k,v pairs for each input
        yield key,val**2
        yield key,val**3
    def reducer(key,vals):
        yield key,sum(vals)

    #  now run the job!
    mapreduce(infile,outfile,mapper=mapper,reducer=reducer)

You can specify a custom parser to read your input,
and a custom formatter to format your output.  The
default_parser and default_formatter read and write
tsv.
"""

from itertools import chain

def default_parser(line):
    """read tab-separated key, val from line"""
    return map(eval,line.strip().split('\t'))

def default_formatter(key,val):
    """format key, val as tsv"""
    if isinstance(key,basestring):
        key = "'"+key+"'"
    if isinstance(val,basestring):
        val = "'"+val+"'"
    return '{0}\t{1}'.format(key,val)

def identity_mapper(key,val):
    """output key, val without change"""
    yield key,val

def identity_reducer(key,vals):
    """output key, val for each value in vals"""
    for val in vals:
        yield key,val

def mapreduce(infile,
              outfile,
              parser=default_parser,
              formatter=default_formatter,
              mapper=identity_mapper,
              reducer=None):
    """run map-reduce job specified by mapper and reducer generator functions"""

    out = open(outfile,'w')
    if not isinstance(infile,list):
        infile = [infile]
    map_out = chain.from_iterable(chain.from_iterable(mapper(*parser(line)) for line in open(f)) \
                  for f in infile)

    if reducer:
        last_key = None
        vals = []
        for key,val in sorted((k,v) for k,v in map_out):
            if key != last_key:
                if last_key is not None:
                    for k,v in reducer(last_key,vals):
                        print >>out,formatter(k,v)
                last_key = key
                vals = []
            vals.append(val)
        for k,v in reducer(last_key,vals):
            print >>out,formatter(k,v)
    else:
        for key,val in map_out:
            print >>out,formatter(key,val)

if __name__ == '__main__':

    def poly_mapper(key,val):
        yield key,val**2
        yield key,val**3

    def sum_reducer(key,vals):
        yield key,sum(vals)

    def wc_mapper(key,val):
        for word in str(val).split():
            yield word,1

    def print_task_output(name,outfile):
        print '{0}:'.format(name)
        for line in open(outfile):
            print line,

    data = [(1,2),(3,4),(1,4),(2,3)]

    infile = 'toydoop.data'
    outfile = 'toydoop.out'

    # write input data file
    f = open(infile,'w')
    for k,v in data:
        print >>f,default_formatter(k,v)
    f.close()
    print_task_output('input',infile)

    # run some map reduce jobs
    mapreduce(infile,outfile,mapper=poly_mapper)
    print_task_output('polynomial mapper',outfile)
    mapreduce(infile,outfile,mapper=poly_mapper,reducer=sum_reducer)
    print_task_output('polynomial mapper and summing reducer',outfile)
    mapreduce(infile,outfile,mapper=wc_mapper,reducer=sum_reducer)
    print_task_output('word count',outfile)


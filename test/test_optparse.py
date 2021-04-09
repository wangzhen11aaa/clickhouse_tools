from optparse import OptionParser

"""
    OptionParser support short option and long option parse,
    When we only use long(double dash) option, *** args in (options, args) *** is empty.
"""
def main():
    #usage = "usage: %prog [options] arg"
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="read data from FILENAME")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose")
    (options, args) = parser.parse_args()
    print options.filename
    print "len(args) " + str(len(args))
    if len(args) != 1:
        parser.error("incorrect number of arguments")
    if options.verbose:
        print("reading %s..." % options.filename)

if __name__ == "__main__":
    main()
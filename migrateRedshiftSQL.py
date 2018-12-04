#!/usr/bin/env python3

import sys

HELP = "./migrateRedshiftSQL.py <redshiftFile>"
CMNT = "#"

def writeVariables(variableList, fout, widgets=False):
    if not widgets:
        fout.write("## Variables used throughout SQL statements\n")
        for v in variableList:
            fout.write("{variable}='{variable}'\n".format(variable=v))
        fout.write("## Variables END\n\n")
    else:
        print("Not implemented yet!")
            

def main(args):
    print("Migrating Redshift SQL to Databricks (PySpark): {}".format(args[0]))
    ## Format filenames in/out
    fileName    = args[1]
    outFileName = fileName[:fileName.rindex(".")] + ".py"
    print("Redshift Filename: {}".format(fileName))
    print("DBR Filename     : {}".format(outFileName))

    ## State variables
    inStmt = False
    inToken = False
    stmt = ""

    ## Code will be placed into a list that keeps getting longer with each line in the file
    ## At the end, we will combine our list of variables with the list of code, so that we
    ## ensure variables are defined at the beginning of the file.
    completeCode = list()
    curStmt = list()
    curStmtVars = set()
    variables = set()

    ## Read input filename line by line.
    f = open(fileName, "r")
    fout = open(outFileName, "w")
    for line in f:
        #print("Len: {} --> ".format(len(line)) + line)
        
        if (line.strip().startswith('\\') or line.strip().startswith("--") or len(line.strip()) == 0):
            # Comment line found
            if not inStmt and len(line.strip()) != 0:
                stmt = CMNT + line
                completeCode.append(stmt)
            else:
                if len(line.strip()) == 0:
                    completeCode.append("\n")
        else:
            # Find all variables that exist in this line and add it to our global set of
            # variables
            i = 0
            for c in line:
                #print("Processing {}".format(c))
                if c == ':':
                    if inToken:
                        #print("End token!")
                        # Save the token into our Set of tokens
                        token = line[tokenStart:i]
                        variables.add(token)
                        curStmtVars.add(token)
                    #print("Starting token!")
                    inToken = True
                    tokenStart = i+1 # Bump token start by one to line up properly
                else:
                    if inToken and not c.isalnum() and c != '_':
                        #print("End token!")
                        inToken = False
                        # Save the token into our Set of tokens
                        token = line[tokenStart:i]
                        variables.add(token)
                        curStmtVars.add(token)
                i = i + 1
            # Replace any variable instances with appropriate braces
            # around the variable.
            for v in variables:
                line = line.replace(":{}".format(v), "{" + v + "}")
            #print("My line is now: {}".format(line))

            # We may have a single line statement, which means we both start
            # and stop the statement in one line.
            if line.strip().endswith(";"):
                # Closing out the statement
                inStmt = False
                curStmt.append(line[:line.rindex(';')]) # Append the line we have to our current stmt
                # Build string that we'll add now to the complete set of code.
                completeCode.append('spark.sql("""')
                for theLine in curStmt:
                    completeCode.append(theLine)
                completeCode.append('""".format(')
                j = 0
                
                for theVar in curStmtVars:
                    completeCode.append(theVar + "=" + theVar)
                    j = j+1
                    if j != len(curStmtVars):
                        completeCode.append(",")

                completeCode.append('))\n')
                curStmt.clear()
                curStmtVars.clear()
            else:
                inStmt = True
                curStmt.append(line)

            # Blanket setting that we must be in a statement if not a comment line
            inStmt = True

    # Variable list
    varList = list(variables)
    varList.sort()

    # Write the variables first in the file
    writeVariables(varList, fout)

    # Write out our code
    for code in completeCode:
        fout.write(code)
        
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(HELP + " num args = {}".format(len(sys.argv)))
        sys.exit(1)
    main(sys.argv)
    sys.exit(0)

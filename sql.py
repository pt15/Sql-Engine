import os
import csv
import sys
import sqlparse
import numpy
import re
from ast import literal_eval

tables = {}
dataTable = {}
DML = ""
fileNotEmpty = 0
dataInserted = 0
tableTrucated = 0


def meta_data():
    metaData = open('metadata.txt')
    currentTableName = ""
    flag = 0
    column = []
    for row in metaData:
        row = row.split('\r')[0];
        row = row.strip("\n")
        if(row == "<begin_table>"):
            flag = 1

        elif(row == "<end_table>"):
            tables[currentTableName] = column
            currentTableName = ""
            column = []

        elif(flag == 1):
            currentTableName = row
            flag = 0

        else:
            column.append(row)

def read_table():
    for tableName in tables.keys():
        table = open(tableName+'.csv', 'r')
        try:
            dataReader = csv.reader(table)
            dataTable[tableName] = []
            for row in dataReader:
                dataTable[tableName].append(row)
        finally:
            table.close()

def sql_parse(query):
    parsed_query = sqlparse.parse(query)[0]
    tokens = parsed_query.tokens
    columnFlag = 0
    fromFlag = 0
    columns = []
    table = []
    where = []
    joinFlag = 0
    DML = ""
    DDL = ""
    colName = []
    tableName = [] 
    intoFlag = 0
    truncFlag = 0
    tableFlag = 0
    valueFlag = 0
    DMLorDDL = 0    #1 for create, 2 for select, 3 for drop, 4 for insert

    for token in tokens:
        if(str(token.ttype) == "Token.Keyword.DML" and token.value == "select"):
            DML = token.value
            DMLorDDL = 2
            columnFlag = 1

        elif columnFlag == 1 and token.ttype == None :
            if token.tokens.__len__() > 1:
                for i in range(0,token.tokens.__len__()):

                    if token.tokens[i].ttype is None and str(token.tokens[i].get_name()).lower() in ['max', 'min', 'sum', 'average', 'distinct']:
                        columns.append(str(token.get_name()))
                        if token.tokens.__len__() == 2:
                            columns.append(str(token.get_parameters()[0].get_name()))
                            joinFlag = 1
                        else:
                            columns.append(str(token.tokens[i].get_parameters()[0].get_name()))
                            joinFlag = 1
                    elif token.tokens[i].ttype is None and joinFlag != 1:
                        columns.append(str(token.tokens[i].value))
                    elif str(token.tokens[i].ttype) == "Token.Name" and joinFlag != 1:
                        columns.append(str(token.value))
                        joinFlag = 1
            else:
                columns.append(str(token.value))
            columnFlag = 0
        elif columnFlag == 1 and str(token.ttype) == "Token.Wildcard":
            columns.append(str(token.value))
            columnFlag = 0
        elif str(token.ttype) == "Token.Keyword" and token.value.lower() == "from":
            fromFlag = 1

        elif fromFlag == 1 and token.ttype == None:
            table = str(token).split(',')
            fromFlag = 0

        elif fromFlag == 0 and columnFlag == 0 and token.ttype == None and DMLorDDL == 2:
            if token.tokens.__len__() == 3:
                temp = token.tokens[2].tokens
                for i in temp:
                    where.append(str(i))

            elif token.tokens.__len__() == 7:
                temp = token.tokens[2].tokens
                for i in temp:
                    where.append(str(i))
                where.append(str(token.tokens[4].value))
                temp = token.tokens[6].tokens
                for i in temp:
                    where.append(str(i))
        elif(str(token.ttype) == "Token.Keyword.DDL" and token.value == "create"):
            DDL = token.value
            DMLorDDL = 1
        elif(str(token.ttype) == "Token.Keyword" and token.value.lower() == "table" and DDL == "create"):
            pass
        elif (str(token.ttype) == "None" and DDL == "create" and DMLorDDL == 1):
            token = re.match(r'(.*)\(.*\)', token.value)
            tableName.append(token.group(1).strip(" "))
            colName = []
            colStr = str(token.group(0))
            colStr = colStr.strip(token.group(1))
            colStr = colStr.strip("(")
            colStr = colStr.strip(")")
            colStr = colStr.split(",")
            for i in colStr:
                i = i.split(" ")
                colName.append(i[0])
        elif(str(token.ttype) == "Token.Keyword.DDL" and token.value == "drop"):
            DDL = token.value
            DMLorDDL = 3
        elif(str(token.ttype) == "Token.Keyword" and token.value.lower() == "table" and DDL == "drop"):
            pass
        elif (str(token.ttype) == "None" and DDL == "drop" and DMLorDDL == 3):
            tableName.append(token.value)
        elif(str(token.ttype) == "Token.Keyword.DML" and token.value == "insert"):
            DML = token.value
            DMLorDDL = 4
        elif(str(token.ttype) == "Token.Keyword" and token.value.lower() == "into" and DML == "insert"):
            intoFlag = 1
        elif(str(token.ttype) == "Token.Keyword" and token.value.lower() == "values" and DML == "insert"):
            valueFlag = 1
        elif (str(token.ttype) == "None" and DML == "insert" and intoFlag == 1):
            table.append(token.value)
            intoFlag = 0
        elif ((str(token.ttype) == "None" and DML == "insert" and valueFlag == 1)):
            columns = literal_eval(token.value)
            valueFlag = 0
        elif(str(token.ttype) == "Token.Keyword" and token.value.lower() == "truncate"):
            truncFlag = 1
            DMLorDDL = 5
            DML = token.value
        elif(str(token.ttype) == "Token.Keyword" and token.value.lower() == "table" and truncFlag == 1):
            tableFlag = 1
        elif (str(token.ttype) == "None" and truncFlag == 1 and tableFlag ==1):
            table.append(token.value)
    if DMLorDDL == 2 or DMLorDDL == 4 or DMLorDDL == 5:                    
        return columns,table, DML, where, DMLorDDL
    elif DMLorDDL == 1 or DMLorDDL == 3:
        return colName, tableName, DDL, where, DMLorDDL

def catch_errors(TABLES, COLUMNS, WHERE, DMLorDDL):
    if DMLorDDL == 1:
        return 1
    if DMLorDDL == 4:
        return 1
    if DMLorDDL == 5:
        for i in TABLES:
            if tables.has_key(i) == False:
                print "Error: No such Table exists"
                return 0
    if TABLES.__len__() == 0:
        print "Error: No Tables Used"
        return 0
    for i in TABLES:
        if tables.has_key(i) == False:
            print "Error: No such Table exists"
            return 0
    for i in COLUMNS:
        if i.find(".") == -1:
            flag = 0
            if i.lower() in ['*', 'max', 'min', 'sum', 'average','distinct']:
                flag = 1
            for j in TABLES:
                if tables[j].count(i) > 0:
                    flag += 1
            if flag == 0:
                print "Error: No such column exists!"
                return 0
            elif flag == 2:
                print "Error: Ambiguous Column "+i
                return 0
        else:
            if i.split(".")[0] not in TABLES:
                print "Unknown Column "+i
                return 0
            if i.split(".")[1] not in tables[i.split(".")[0]]:
                print "Unknown Column "+i
                return 0
        for i in WHERE:
            if i.find(".") != -1:
                if tables.has_key(i.split(".")[0]) == False:
                    print "Unknown column "+i+" in 'where clause'"
                    return 0
                if i.split(".")[1] not in tables[i.split(".")[0]]:
                    print "Unknown column "+i+" in 'where clause'"
                    return 0
            else:
                flag = 0
                if i.isdigit() == True or (i[0] == '-' and i[1:].isdigit() == True):
                    flag = 1
                if i in ['=','>','<','>=','<=']:
                    flag = 1
                elif i.lower() in ['and','or']:
                    flag = 1
                for j in TABLES:
                    if i in tables[j]:
                        flag = 1
                if flag == 0:
                    print "Unknown column "+i+" in 'where clause'"
                    return 0


def execute(TABLES, COLUMNS, DML, WHERE):
    global fileNotEmpty
    global dataInserted
    global tableTrucated
    output = []
    tempOutput = []
    temp = []
    sum = 0
    function = {}
    isFunction = None
    productFlag = 0
    joinFlag = 0
    joinFlagConfirm = 0
    distinctFlag = 0
    countColumn = {}
    if DML.upper() == 'CREATE':
        with open("metadata.txt", "a") as myfile:
            myfile.write("\n<begin_table>\n")
            myfile.write(TABLES[0] + "\n")
            for i in COLUMNS:
                myfile.write(i + "\n")
            myfile.write("<end_table>")
        myfile.close()
        a = open(TABLES[0]+".csv", 'w')
        a.close()
    elif DML.upper() == "DROP":
        if os.stat(TABLES[0]+".csv").st_size == 0:
            os.remove(TABLES[0]+".csv")
        else:
            fileNotEmpty = 1
            print "File is not empty"
    elif DML.upper() == 'INSERT':
        insertData = open(TABLES[0]+".csv","a")
        if os.stat(TABLES[0]+".csv").st_size == 0:
            print "+1"
            pass
        else:
            insertData.write("\n")
        i = 0
        for col in columns:
            if i == 0:
                insertData.write(str(col))
            else:
                insertData.write("," + str(col))
            i = i + 1
        insertData.close()
        dataInserted = 1
    elif DML.upper() == 'TRUNCATE':
        trucateData = open(TABLES[0]+".csv", "w")
        trucateData.truncate()
        tableTrucated = 1
        trucateData.close()
    elif DML.upper() == 'SELECT' and WHERE.__len__() == 0:
        for i in TABLES:
            Index = []
            isFunction = None
            function = {}
            countColumn[i] = []
            for j in tables[i]:
                countColumn[i].append(0)
            for j in COLUMNS:
                if j.lower() in ['max', 'min', 'sum', 'average', 'distinct']:
                    isFunction = j
                elif j == '*':
                    for k in range(0,tables[i].__len__()):
                        Index.append(k)
                elif j.find(".") != -1:
                    temp = j.split(".")
                    if temp[0] == i:
                        Index.append(tables[i].index(temp[1]))
                else:
                    try:
                        Index.append(tables[i].index(j))
                    except:
                        productFlag = 1

                    if isFunction != None:
                        function[tables[i].index(j)] = []
                        function[tables[i].index(j)].append(isFunction)
                    isFunction = None
            for j in dataTable[i]:
                temp = []
                for k in Index:
                    temp.append(int(j[k]))
                    countColumn[i][k] += 1
                output.append(temp)
            for k in Index:
                if function.has_key(k) == True:
                    if function[k][0].lower() == 'max':
                        output.sort()
                        output.reverse()
                        output = output[0]
                    elif function[k][0].lower() == 'min':
                        output.sort()
                        output = output[0]
                    elif function[k][0].lower() == 'sum':
                        for l in output:
                            sum = sum + int(l[0])
                        output = sum
                    elif function[k][0].lower() == 'average':
                        for l in output:
                            sum = sum + int(l[0])
                        output = sum*1.0/(output.__len__())
                    #TODO: Implement Distinct
                    elif function[k][0].lower() == 'distinct':
                        distinctFlag = 1
                        tempOut = []
                        for l in output:
                            if l not in tempOut:
                                tempOut.append(l)
                        output = tempOut

    elif DML.upper() == 'SELECT' and WHERE.__len__() > 0:
        whereIndex = {}
        ANDorOR = {}
        comparisonOperator = {}
        present = 0
        for i in TABLES:
            whereIndex[i] = []
            ANDorOR[i] = []
            comparisonOperator[i] = []
            for j in WHERE:
                if j in tables[i]:
                    present = 1
                    whereIndex[i].append(tables[i].index(j))
                elif j.isdigit() is True or (j[0] == '-' and j[1:].isdigit() == True):
                    if whereIndex[i].__len__() > 0 and (type(whereIndex[i][-1]).__name__ == 'int' or whereIndex[i][-1].__class__() == []):
                        whereIndex[i].append(j)
                    joinFlagConfirm = 1
                elif j.find('.') != -1:
                    joinFlag = 1
                    present = 1
                    temp = j.split('.')
                    whereIndex[i].append(temp)
                elif j in ['>','<','=','>=','<='] and present == 1:
                    present = 0
                    if j != "=":
                        joinFlagConfirm = 1
                    comparisonOperator[i].append(j)
                elif j.lower() in ['and', 'or']:
                    joinFlagConfirm = 1
                    ANDorOR[i].append(j.lower())
        for i in TABLES:
            Index = []
            countColumn[i] = []
            for j in tables[i]:
                countColumn[i].append(0)
            for j in COLUMNS:
                if j.lower() in ['max', 'min', 'sum', 'average', 'distinct']:
                    isFunction = j
                elif j == '*':
                    for k in range(0,tables[i].__len__()):
                        Index.append(k)
                elif j.find(".") != -1:
                    temp = j.split(".")
                    if temp[0] == i:
                        Index.append(tables[i].index(temp[1]))
                    productFlag = 1
                else:
                    try:
                        Index.append(tables[i].index(j))
                    except:
                        pass

                    if isFunction != None:
                        function[tables[i].index(j)] = []
                        function[tables[i].index(j)].append(isFunction)

            for j in dataTable[i]:
                temp = []
                flagTemp = 0
                tempWhere = []
                tempComp = []
                flagWhere = 0
                tempWhere = whereIndex[i]
                tempANDorOR = ANDorOR[i]
                tempComp = comparisonOperator[i]
                if joinFlag == 0 or joinFlagConfirm == 1:
                    if joinFlag == 1:
                        for l in range(0, tempWhere.__len__()):
                            temp = []
                            if tempWhere[l].__class__() == []:
                                if tempWhere[l][0] == i:
                                    temp.append(tables[i].index(tempWhere[l][1]))
                                    tempWhere[l] = temp[0]
                                else:
                                    tempWhere[l] = None
                    tempflag = 0
                    for l in tempWhere:
                        if l == None:
                            tempComp.remove(tempComp[(tempWhere.index(l))/2])
                            tempflag = 1
                            tempWhere.remove(l)
                        elif tempflag == 1:
                            tempWhere.remove(tempWhere[tempWhere.index(l)-1])

                    for l in range(0,tempWhere.__len__()/2):
                        if l == 1:
                            if tempANDorOR[0] == 'and':
                                if flagWhere == 1:
                                    break
                            elif tempANDorOR[0] == 'or':
                                if flagWhere == 0:
                                    break
                                else:
                                    flagWhere = 0
                        if tempComp[l] == '>':
                            if joinFlag != 1:
                                if type(tempWhere[2*l + 1]).__name__ != 'int' and (tempWhere[2*l + 1].isdigit() == True or (tempWhere[2*l + 1][0] == '-' and tempWhere[2*l + 1][1:].isdigit() == True)) and int(j[tempWhere[2*l]]) <= int(tempWhere[2*l + 1]):
                                    flagWhere = 1
                                elif type(tempWhere[2*l + 1]).__name__ == 'int' and int(j[tempWhere[2*l]]) <= int(j[tempWhere[2*l + 1]]):
                                    flagWhere = 1
                            else:
                                if type(tempWhere[2*l + 1]).__name__ != 'int' and (tempWhere[2*l + 1].isdigit() == True or (tempWhere[2*l + 1][0] == '-' and tempWhere[2*l + 1][1:].isdigit() == True)) and int(j[tempWhere[2*l]]) <= int(tempWhere[2*l + 1]):
                                    flagWhere = 1
                                elif type(tempWhere[2*l + 1]).__name__ == 'int' and int(j[tempWhere[2*l]]) <= int(j[tempWhere[2*l + 1]]):
                                    flagWhere = 1

                        elif tempComp[l] == '<':
                            if type(tempWhere[2*l + 1]).__name__ != 'int' and (tempWhere[2*l + 1].isdigit() == True or (tempWhere[2*l + 1][0] == '-' and tempWhere[2*l + 1][1:].isdigit() == True)) and int(j[tempWhere[2*l]]) >= int(tempWhere[2*l + 1]):
                                flagWhere = 1
                            elif type(tempWhere[2*l + 1]).__name__ == 'int' and int(j[tempWhere[2*l]]) >= int(j[tempWhere[2*l + 1]]):
                                flagWhere = 1

                        elif tempComp[l] == '=':
                            if type(tempWhere[2*l + 1]).__name__ != 'int' and (tempWhere[2*l + 1].isdigit() == True or (tempWhere[2*l + 1][0] == '-' and tempWhere[2*l + 1][1:].isdigit() == True)) and int(j[tempWhere[2*l]]) != int(tempWhere[2*l + 1]):
                                flagWhere = 1
                            elif type(tempWhere[2*l + 1]).__name__ == 'int' and int(j[tempWhere[2*l]]) != int(j[tempWhere[2*l + 1]]):
                                flagWhere = 1

                        elif tempComp[l] == '>=':
                            if type(tempWhere[2*l + 1]).__name__ != 'int' and (tempWhere[2*l + 1].isdigit() == True or (tempWhere[2*l + 1][0] == '-' and tempWhere[2*l + 1][1:].isdigit() == True)) and int(j[tempWhere[2*l]]) < int(tempWhere[2*l + 1]):
                                flagWhere = 1
                            elif type(tempWhere[2*l + 1]).__name__ == 'int' and int(j[tempWhere[2*l]]) < int(j[tempWhere[2*l + 1]]):
                                flagWhere = 1

                        elif tempComp[l] == '<=':
                            if type(tempWhere[2*l + 1]).__name__ != 'int' and (tempWhere[2*l + 1].isdigit() == True or (tempWhere[2*l + 1][0] == '-' and tempWhere[2*l + 1][1:].isdigit() == True)) and int(j[tempWhere[2*l]]) > int(tempWhere[2*l + 1]):
                                flagWhere = 1
                            elif type(tempWhere[2*l + 1]).__name__ == 'int' and int(j[tempWhere[2*l]]) > int(j[tempWhere[2*l + 1]]):
                                flagWhere = 1
                    if flagWhere == 0:
                        for k in Index:
                            temp.append(int(j[k]))
                            countColumn[i][k] += 1
                        flagTemp = 1
                elif joinFlag == 1 and joinFlagConfirm == 0:
                    columnOne = []
                    columnTwo = []
                    flagList = []
                    joinFlag = 2
                    for l in dataTable[tempWhere[0][0]]:
                        columnOne.append([int(l[tables[tempWhere[0][0]].index(tempWhere[0][1])]), l])

                    for l in dataTable[tempWhere[1][0]]:
                        columnTwo.append([int(l[tables[tempWhere[1][0]].index(tempWhere[1][1])]), l])

                    for l in columnOne:
                        for m in columnTwo:
                            if tempComp[0] == '=':
                                if l[0] == m[0]:
                                    temp = []
                                    for n in COLUMNS:
                                        if n != "*":
                                            if n.find(".") == -1:
                                                if tables[tempWhere[0][0]].count(n) > 0:
                                                    temp.append(l[1][tables[tempWhere[0][0]].index(n)])

                                                elif tables[tempWhere[1][0]].count(n) > 0:
                                                    temp.append(m[1][tables[tempWhere[1][0]].index(n)])
                                            else:
                                                if tempWhere[0][0] == n.split(".")[0]:
                                                    temp.append(l[1][tables[tempWhere[0][0]].index(n.split(".")[1])])
                                                else:
                                                    temp.append(m[1][tables[tempWhere[1][0]].index(n.split(".")[1])])
                                        else:
                                            for o in l[1]:
                                                temp.append(o)
                                            for o in range(0, m[1].__len__()):
                                                if o != tables[tempWhere[1][0]].index(tempWhere[1][1]):
                                                    temp.append(m[1][o])

                                    output.append(temp)

                    temp = []
                    if COLUMNS[0] == "*":
                        for l in tables[TABLES[0]]:
                            temp.append(TABLES[0])
                            temp.append(l)
                        for l in tables[TABLES[1]]:
                            if l != tempWhere[1][1]:
                                temp.append(TABLES[1])
                                temp.append(l)
                        COLUMNS = temp

                if flagTemp == 1:
                    output.append(temp)
                    flagWhere = 0

            for k in Index:

                if productFlag != 1 and function.has_key(k) == True:
                    if function[k][0].lower() == 'max':
                        output.sort()
                        output.reverse()
                        output = output[0]
                    elif function[k][0].lower() == 'min':
                        output.sort()
                        output = output[0]
                    elif function[k][0].lower() == 'sum':
                        for l in output:
                            sum = sum + int(l[0])
                        output = sum
                    elif function[k][0].lower() == 'average':
                        for l in output:
                            sum = sum + int(l[0])
                        output = sum*1.0/(output.__len__())
                    elif function[k][0].lower() == 'distinct':
                        distinctFlag = 1
                        tempOut = []
                        for l in output:
                            if l not in tempOut:
                                tempOut.append(l)
                        output = tempOut
        joinFlag = 2
    #elif DML.upper() == "CREATE":

    return output,productFlag,distinctFlag,countColumn,COLUMNS



if __name__ == '__main__':
    columns,table,DML,where, DMLorDDL = sql_parse(sys.argv[1])
    if DML == "create":
        pass
    else:
        meta_data()
        read_table()
    result = catch_errors(table, columns, where, DMLorDDL)
    #result = 1
    if result != 0:
        output, productFlag, distinctFlag, countCol, columns = execute(table, columns, DML, where)
        if DML == "create":
            print "Table " + table[0] + " created."
            exit()
        if DML == "drop":
            if fileNotEmpty:
                print "Table" + table[0] + " is not empty!"
            else:
                print "Table " + table[0] + " deleted."
            exit()
        if DML == "insert":
            if dataInserted:
                print "Values " + str(columns) + " inserted into table " + table[0]
            else:
                print "Some error"
            exit()
        if DML == "truncate":
            if tableTrucated:
                print "Data from " + table[0] + ".csv deleted."
            else:
                print "Some error"
            exit()
        printCol = "< "
        if columns != [] and columns[0] == '*':
            columns = []
            for i in tables[table[0]]:
                columns.append(i)
        if columns != [] and columns[0].lower() in ['min', 'max', 'sum', 'average']:
            printCol = printCol + columns[0] + "(" + table[0]+"."+columns[1] + ")" + " >"
            print printCol
            if output.__class__() == []:
                print output[0]
            else:
                print output
        elif distinctFlag == 1:
            for i in range(0,columns.__len__()/2):
                if i == 0:
                    printCol = printCol + columns[2*i] + "(" + table[0] + "." + columns[2*i+1] + ")"
                else:
                    printCol = printCol + ", " + columns[2*i] + "(" + table[0] + "." + columns[2*i+1] + ")"
            printCol += " >"
            print printCol
            for i in output:
                temp = ""
                for j in range(0,i.__len__()):
                    if(j == i.__len__()-1):
                        temp += str(i[j])
                    else:
                        temp += str(i[j])+", "
                print temp
        else:
            #TODO: CHANGE THIS TO PRODUCTFLAG==0 AND IMPLEMENT MULTIPLICATION
            if productFlag == 1 or productFlag == 0:
                for i in range(0,columns.__len__()):
                    if i == 0:
                        if table.__len__() == 1:
                            if columns[i].find(".") == -1:
                                printCol += table[0]+"."+columns[i]
                            else:
                                printCol += columns[i]
                        else:
                            if columns[i] in table:
                                printCol += columns[i]+"."
                            elif columns[i-1] in table:
                                printCol += columns[i]
                            else:
                                printCol += columns[i]
                    else:
                        if table.__len__() == 1:
                            if columns[i].find(".") == -1:
                                printCol = printCol + ", " + table[0] +"."+ columns[i]
                            else:
                                printCol += ", " + columns[i]
                        else:
                            if columns[i] in table:
                                printCol += ", " + columns[i] + "."
                            elif columns[i-1] in table:
                                printCol += columns[i]
                            else:
                                printCol = printCol + ", " + columns[i]
                printCol += " >"
                print printCol
                if output.__len__() > 0 and columns.__len__() - output[0].__len__() == 1 and output[0].__len__() > 0:
                    tempTable = {}
                    tempFlag = 0
                    for i in columns:
                        if i.find(".") == -1:
                            tempTable[i] = None
                        else:
                            tempFlag = 1
                            tempTable[i.split(".")[1]] = i.split(".")[0]
                        for j in table:
                            if tempFlag == 0:
                                if tables[j].count(i) > 0:
                                    tempTable[i] = j
                    if tempFlag == 0:
                        count1 = countCol[tempTable[columns[0]]][tables[tempTable[columns[0]]].index(columns[0])]
                        count2 = countCol[tempTable[columns[1]]][tables[tempTable[columns[1]]].index(columns[1])]
                    else:
                        count1 = countCol[tempTable[columns[0].split(".")[1]]][tables[tempTable[columns[0].split(".")[1]]].index(columns[0].split(".")[1])]
                        count2 = countCol[tempTable[columns[1].split(".")[1]]][tables[tempTable[columns[1].split(".")[1]]].index(columns[1].split(".")[1])]
                    for j in range(0, count1):
                        for k in range(count1, count2+count1):
                            temp = ""
                            temp += str(output[j][0])+", "+str(output[k][0])
                            print temp

                else:
                    for i in output:
                        temp = ""
                        for j in range(0,i.__len__()):
                            if(j == i.__len__()-1):
                                temp += str(i[j])
                            else:
                                temp += str(i[j])+", "
                        if temp != "":
                            print temp


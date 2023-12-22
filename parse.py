import os
from pmydb.core.database import Database
from pmydb.core.collection import Collection
from pmydb.core.mergeSort import mergeSort
from pmydb.core.mergeAggr import mergeAggr
from pmydb.core.joinColls import joinColls
from shutil import copyfile


# dbName$collName$insertDoc/deleteDocs/addField/deleteField/updateDocs/search/sort/groupBy
# insertDoc(doc={}); addField(name:str=xx, value:(str, int, float)=xx);
# deleteDocs(filter:str=[xx], 应当以[.. and .. or ..]为格式，且字段前面都要加上".//"); deleteField(name:str=xx)
# updateDocs(filter:str=[xx], 规则同上; name:str=xx, value=xx, append:bool=False, append用于listField)
# search(filter:str=[xx], groupOn:list[str]=["xxx"], func:str=xxx, aggrField:str=xxx,
#   sort:dict={"None":False}, project:dict[str, str]={"":""}, distinct:bool=xxx, offset:int=x, limit:int=x)
# groupBy(groupOn:list=["None"], func:str=xxx, aggrField:str=xxx)
# join(foreignColl:command=a new command with sep="$", localField:str=xxx, foreignField:str=xxx)
def parse(command:str, sep:str="$", next:bool=False):

    def delTmpFile(dirPath, collname):
        ls = os.listdir(dirPath)
        for i in ls:
            if (collname+"_" not in i):
                os.remove(os.path.join(dirPath, i))
                


    def del_files(path):
        ls = os.listdir(path)
        for i in ls:
            f_path = os.path.join(path, i)
            # 判断是否是一个目录,若是,则递归删除
            if os.path.isdir(f_path):
                del_files(f_path)
                os.rmdir(f_path)
            else:
                os.remove(f_path)
                

    # transfer to dbname; drop dbname
    if (sep not in command):
        dbName = command.split(" ")[-1]
        if ("transfer" in command):
            if not os.path.exists(os.path.join(dbName)):
                os.makedirs(os.path.join(dbName))
            # 再建一个存db内coll的元数据的文件
            collInfoFilePath = os.path.join(dbName, "collInfos.txt")
            if not os.path.exists(collInfoFilePath):
                open(collInfoFilePath, "w")
            return ("Transfered done.")
        elif ("drop" in command):
            del_files(os.path.join(dbName))
            os.rmdir(os.path.join(dbName))
            return ("Database drop done.")
    
    # dbname.showCollections()
    # dbname.createCollection(name:str=xxx, priKey:list[str]=[xxx,xxx], listField:str=xxx)
    # dbname.dropCollection(name:str=xxx)
    secList = command.split(sep)
    if (len(secList) == 2):
        dbName = secList[0]
        db = Database(name=dbName, path=dbName)
        if (secList[1] == "showCollections()"):
            try:
                return (db.getAttr()["collectionsName"])
            except FileNotFoundError:
                return ([])
        elif ("createCollection" in secList[1]):
            body = secList[1]
            body = body[(body.index("(")+1):-1]
            collName = eval(body.split("name=")[1].split(",")[0].strip())
            priKey = eval("[" + body.split("priKey=[")[1].split("]")[0] + "]") if "priKey" in body else ["None"]
            listField = eval(body.split("listField=")[1]) if "listField" in body else "None"
            return (db.createColl(collName=collName, priKey=priKey, listField=listField))
        elif ("dropCollection" in secList[1]):
            body = secList[1]
            body = body[(body.index("(")+1):-1]
            collName = eval(body.split("name=")[1].split(",")[0].strip())
            return (db.dropColl(collName=collName))
    
    dbName = secList[0]
    collName = secList[1]
    db = Database(name=dbName, path=os.path.join(dbName))
    collInfos = db.getAttr()["collectionsInfo"][collName]

    # dbname.collname.(insertDoc/deleteDocs/addField/deleteField/updateDocs/search)(xxx)
    if (len(secList) == 3):
        sec = secList[2]
        action = sec.split("(")[0]
        i1 = sec.index("(")
        body = sec[(i1+1):-1]  # 这里已经把外层的括号去掉了
        if (action == "insertDoc"):
            docDict = eval(body.split("=")[1])
            fileList = os.listdir(os.path.join(dbName, collName))
            fileList.sort(key = lambda x: eval(x.split("_")[1].split(".")[0]))
            # flag = 1
            if (collInfos["priKey"] != ["None"]):
                for fileName in fileList:
                    c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                           readPath=os.path.join(dbName, collName, fileName),
                           writePath=os.path.join(dbName, collName, fileName))
                    indicator = c.insertDoc(docDict=docDict, write=False)
                    if (indicator == 0):
                        # flag = 0
                        return (0)
            # if (flag == 1):
            c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                    readPath=os.path.join(dbName, collName, fileList[-1]),
                    writePath=os.path.join(dbName, collName, fileList[-1]))
            indicator = c.insertDoc(docDict=docDict, write=True)
            if (indicator == 0):
                return (0)
            if (c.getAttr()["listField"] != collInfos["listField"]):
                collInfos["listField"] = c.getAttr()["listField"]
                db.changeListField(collName=collName, listField=c.getAttr()["listField"])
            # print ("Insertion is done.")
            return (1)
        elif (action == "deleteDocs"):
            query = "[" + body.split("filter=")[1].split("[")[1].split("]")[0] + "]"
            fileList = os.listdir(os.path.join(dbName, collName))
            fileList.sort(key = lambda x: eval(x.split("_")[1].split(".")[0]))
            first = fileList[0]
            for fileName in fileList:
                c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                        readPath=os.path.join(dbName, collName, fileName),
                        writePath=os.path.join(dbName, collName, fileName))
                c.delDocs(query=query, write=True)
                if (c.search() == [] and fileName != first):
                    os.remove(os.path.join(dbName, collName, fileName))
            return (1)
        elif (action == "addField"):
            field_name = eval(body.split("=")[1].split("value")[0].strip().strip(","))
            value = body.split("value=")[1]
            value = eval(value)
            fileList = os.listdir(os.path.join(dbName, collName))
            fileList.sort(key = lambda x: eval(x.split("_")[1].split(".")[0]))
            for fileName in fileList:
                c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                        readPath=os.path.join(dbName, collName, fileName),
                        writePath=os.path.join(dbName, collName, fileName))
                indicator = c.addField(field_name=field_name, value=value, write=True)
                if (indicator == 0):
                    return (0)
            return (1)
        elif (action == "deleteField"):
            field_name = eval(body.split("name=")[1])
            fileList = os.listdir(os.path.join(dbName, collName))
            fileList.sort(key = lambda x: eval(x.split("_")[1].split(".")[0]))
            for fileName in fileList:
                c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                        readPath=os.path.join(dbName, collName, fileName),
                        writePath=os.path.join(dbName, collName, fileName))
                indicator = c.delField(field_name=field_name, write=True)
                if (indicator == 0):
                    return (0)
            return (1)
        elif (action == "updateDocs"):
            query = "[" + body.split("filter=")[1].split("[")[1].split("]")[0] + "]"
            field_name = eval(body.split("name=")[1].split("value")[0].strip().strip(","))
            field_value = eval(body.split("value=")[1].split("append")[0].strip().strip(","))
            append = eval(body.split("append=")[1]) if ("append=" in body) else False
            # output.append([comm_func[action], query, field_name, field_value, append])
            fileList = os.listdir(os.path.join(dbName, collName))
            fileList.sort(key = lambda x: eval(x.split("_")[1].split(".")[0]))
            for fileName in fileList:
                c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                        readPath=os.path.join(dbName, collName, fileName),
                        writePath=os.path.join(dbName, collName, fileName))
                indicator = c.updateDocs(query=query, field_name=field_name, field_value=field_value,
                                         append=append, write=True)
                if (indicator == 0):
                    return (0)
                if (c.search() == []):
                    os.remove(os.path.join(dbName, collName, fileName))
            return (1)
        elif (action == "search"):
            query = "[" + body.split("filter=")[1].split("[")[1].split("]")[0] + "]"
            groupBy = eval(body.split("groupOn=")[1].split("func=")[0].strip().strip(",")) if "groupOn=" in body else ["None"]
            func = eval(body.split("func=")[1].split(",")[0]) if "func=" in body else "None"
            aggrField = eval(body.split("aggrField=")[1].split(",")[0]) if "aggrField=" in body else "None"
            sort = eval(body.split("sort=")[1].split("}")[0] + "}") if ("sort=" in body) else {"None":False}
            sort = [[k, v] for k, v in sort.items()]
            project = eval(body.split("project=")[1].split("}")[0] + "}") if ("project=" in body) else {"None":"None"}
            distinct = eval(body.split("distinct=")[1].split(",")[0]) if ("distinct=" in body) else False
            offset = eval(body.split("offset=")[1].split(",")[0]) if ("offset=" in body) else 0
            limit = eval(body.split("limit=")[1].split(",")[0]) if ("limit=" in body) else 0
            # output.append([comm_func[action], query, sort, project, distinct, offset, limit])
            fileList = os.listdir(os.path.join(dbName, collName))
            fileList.sort(key = lambda x: eval(x.split("_")[1].split(".")[0]))
            tmpFiles, ind = [], 0
            for fileName in fileList:
                ind += 1
                # tmp_file_path = tempfile.mkstemp(prefix='search_'+collName, suffix='.txt',
                #                                     dir=os.path.join(dbName, collName), text=True)
                tmp_file_path = os.path.join(dbName, collName, str(ind)+".txt")
                tmpFiles.append(tmp_file_path)
                c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                        readPath=os.path.join(dbName, collName, fileName), writePath=tmp_file_path)
                if (func == "None"):
                    c.search(query=query, groupBy=groupBy, func=func, aggrField=aggrField, sort=sort,
                            project=project, distinct=distinct, offset=offset, limit=limit, 
                            toXml=False, write=True)
                else:
                    c.search(query=query, groupBy=groupBy, func=func, aggrField=aggrField, sort=sort,
                            project={"None":"None"}, distinct=distinct, offset=offset, limit=limit, 
                            toXml=False, write=True)
            
            if (func == "None"):
                if (project != {"None":"None"}):
                    sort = [[project[s[0]], s[1]] for s in sort if s[0] in project.keys()]
                    if (sort == []):
                        sort=[["None", False]]

            # first = tmpFiles[0]
            final = os.path.join(dbName, collName, "mergeSort.txt")
            middle = os.path.join(dbName, collName, "mergeSortMiddle.txt")
            copyfile(tmpFiles[0], final)

            if (len(tmpFiles) > 1):
                for follow in tmpFiles[1:]:
                    if (func == "None"):
                        mergeSort(leftPath=final, rightPath=follow, toPath=middle,
                                  sort=sort, distinct=distinct)
                        # os.remove(first)
                        copyfile(middle, final)
                        os.remove(middle)
                        # os.rename(tmp_file_path, first)
                    else:
                        mergeAggr(leftPath=final, rightPath=follow, sort=sort, groupBy=groupBy, func=func,
                                  aggrField=aggrField)
                    # os.rename(tmp_file_path, first)
            # for f in tmpFiles:
            #     os.remove(f)
            
            # final = os.path.join(dbName, collName, "search.txt")
            # first = tmp_file_path
            if (func == "None"):
                c = Collection(mainField=collName.lower(), priKey=collInfos["priKey"], listField=collInfos["listField"],
                            readPath=final, writePath=final)
                if next:
                    return (final)
                else:
                    delTmpFile(dirPath=os.path.join(dbName, collName), collname=collName)
                    # os.remove(final)
                    return (c.search(distinct=distinct, offset=offset, limit=limit))
            else:
                # aggr就不要有project了
                if (func != "avg"):
                    c = Collection(mainField=collName.lower(), priKey=groupBy, listField="None",
                            readPath=final, writePath=final)
                    if next:
                        return (final)
                    else:
                        delTmpFile(dirPath=os.path.join(dbName, collName), collname=collName)
                        # os.remove(final)
                        return (c.search(sort=sort))
                
                result = []
                with open(final, "r") as f:
                    for line in f:
                        doc = eval(line.strip("\n"))
                        doc["avg_"+aggrField] = doc["trueAvg"]
                        doc.pop("lengthDocs")
                        doc.pop("trueAvg")
                        result.append(doc)
                with open(final, "w") as f:
                    for d in result:
                        f.write(str(d) + "\n")
                # if next:
                #     return (final)
                delTmpFile(dirPath=os.path.join(dbName, collName), collname=collName)
                # os.remove(final)
                return (result)
                

    # dbname.collname.search(xxx).join(xxx)。默认左连接，如果想要inner join，相当于左边需要筛一波存在那个字段的doc。
    elif (len(secList) == 4):
        # leftPath = parse(command=sep.join(secList[:3]), sep=sep, next=True).split("\\")[-1]
        leftPath = parse(command=sep.join(secList[:3]), sep=sep, next=True)

        join = secList[3]
        i1 = join.index("(")
        body = join[(i1+1):-1]  # 这里已经把外层的括号去掉了

        foreignColl = body.split("foreignColl=")[1].split("localField=")[0].strip().strip(",")
        rightCollName = foreignColl.split(".")[1]
        # rightPath = parse(command=foreignColl, sep=".", next=True).split("\\")[-1]
        rightPath = parse(command=foreignColl, sep=".", next=True)
        leftField = eval(body.split("localField=")[1].split(",")[0])
        rightField = eval(body.split("foreignField=")[1].split(")")[0])
        # print(foreignColl, rightCollName, rightPath, rightField)
        to = os.path.join(dbName, collName+"_join_"+rightCollName+".txt")

        joinColls(leftPath=leftPath, rightPath=rightPath, toPath=to, leftMainField=collName.lower(),
                  rightMainField=rightCollName.lower(), leftField=leftField, rightField=rightField)
        
        result = []
        with open(to, "r") as f:
            for line in f:
                result.append(eval(line.strip("\n")))
        os.remove(to)
        delTmpFile(dirPath=os.path.join(dbName, collName), collname=collName)
        delTmpFile(dirPath=os.path.join(dbName, rightCollName), collname=rightCollName)
        return (result)


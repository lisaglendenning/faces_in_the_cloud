#
# The following environment variables must be defined:
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
#


from boto.sdb.connection import SDBConnection


class Table:
    
    def __init__(self, id, name):
        self.id = id
        self.name = name
    
    def get_name(self):
        return self.name
    
    def get_id(self):
        return self.id


class Catalog:
    DOMAIN = "cloudvision"
    CATALOG_ITEM = "catalog"
    NEXT_TABLE_ID_ATTR = "next_table_id"
    TABLES_ATTR = "tables"
    TABLES_DELIM = ","
    TABLES_ASSIGN = ":"
    TABLE_NAME_ATTR = "name"

    def __init__(self):
        self.conn = None
        self.dom = None
    
    def connect(self):
        self.conn = SDBConnection()
        self.dom = self.conn.get_domain(self.DOMAIN)
    
    def get_tables(self):
        tables = [ ]
        tables_str = self.get_tables_assign()
        for t in tables_str:
            tname, tid = t.split(self.TABLES_ASSIGN)
            tid = int(tid)
            tables.append(Table(tid, tname))
        return tables
    
    def get_tables_assign(self):
        catalog = self.dom.get_item(self.CATALOG_ITEM)
        if len(catalog[self.TABLES_ATTR]) > 0:
            return catalog[self.TABLES_ATTR].split(self.TABLES_DELIM)
        return [ ]
                
    def get_table_by_name(self, name):
        assert name is not None
        tables = self.get_tables()
        for t in tables:
            if t.get_name() == name:
                return t
        return None
    
    def get_table_by_id(self, id):
        assert id is not None
        tables = self.get_tables()
        for t in tables:
            if t.get_id() == id:
                return t
        return None
    
    def put_table(self, name):
        assert name is not None
        catalog = self.dom.get_item(self.CATALOG_ITEM)
        tables = self.get_tables_assign()
        for t in tables:
            tname, tid = t.split(self.TABLES_ASSIGN)
            assert tname != name
        id = int(catalog[self.NEXT_TABLE_ID_ATTR])
        tables.append(self.TABLES_ASSIGN.join([name, str(id)]))
        self.dom.put_attributes(self.CATALOG_ITEM,
                                { self.TABLES_ATTR : self.TABLES_DELIM.join(tables),
                                  self.NEXT_TABLE_ID_ATTR : str(id + 1) })
        self.dom.put_attributes(str(id), { })
        return Table(id, name)
        
    def del_table(self, table):
        assert table is not None
        catalog = self.dom.get_item(self.CATALOG_ITEM)
        tables = self.get_tables_assign()
        for i in range(len(tables)):
            tname, tid = tables[i].split(self.TABLES_ASSIGN)
            tid = int(tid)
            if tname == table.get_name():
                del tables[i]
                break
        else:
            assert False
        self.dom.put_attributes(self.CATALOG_ITEM,
                                { self.TABLES_ATTR : self.TABLES_DELIM.join(tables) })
        if table.get_id() == int(catalog[self.NEXT_TABLE_ID_ATTR]) - 1:
            self.dom.put_attributes(self.CATALOG_ITEM, 
                                    { self.NEXT_TABLE_ID_ATTR : str(table.get_id()) })
        t = self.dom.get_item(str(table.get_id()))
        assert t is not None
        self.dom.delete_item(t)
        
        
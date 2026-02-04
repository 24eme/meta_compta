from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
import glob, sys, os, hashlib
import sqlite3
import re
import time
from datetime import date
from dateutil.relativedelta import relativedelta
from django.db import models
from lxml import etree

class Indexer(object):

    @staticmethod
    def homogeneise_meta(meta):
        for m in meta:
            if isinstance(meta[m], bytes):
                if meta[m][:2] == b'\xfe\xff':
                    meta[m] = meta[m][2:].decode('utf-16be')
                elif meta[m][:2] == b'\xff\xfe':
                    meta[m] = meta[m][2:].decode('utf-16le')
                else:
                    try:
                        meta[m] = meta[m].decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            meta[m] = meta[m].decode('iso8859-1')
                        except UnicodeDecodeError:
                            try:
                                meta[m] = meta[m].decode('latin1')
                            except UnicodeDecodeError:
                                meta[m] = meta[m].decode('ascii')
            if str(meta[m]).find('Ã') == -1:
                continue
            try:
                meta[m] = meta[m].encode('iso8859-1').decode('utf-8')
            except Exception as e:
                raise Exception("Erreur d'indexation "+str(meta)+" "+str(e))
        return meta

    @staticmethod
    def index_image(file, last, conn):
        excludes = os.environ.get('COMPTA_PDF_EXCLUDE_PATH', '')
        for exclude in excludes.split('|'):
            if exclude and file.find(exclude) > -1:
                return False

        res = conn.execute("SELECT id FROM pdf_file where fullpath = \"%s\"" % file.replace('png', 'pdf').replace('jpg', 'pdf').replace('jpeg', 'pdf'));
        for row in res:
            conn.execute("DELETE FROM pdf_file where fullpath = \"%s\"" % file);
            return False

        mtime = os.path.getmtime(file)
        if  mtime <= last:
            return False

        fp = open(file, 'rb')
        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: fp.read(4096), b""):
            hash_md5.update(chunk)

        meta = {}
        meta['md5'] = hash_md5.hexdigest()
        meta['ctime'] = os.path.getctime(file)
        meta['mtime'] = mtime
        filename = os.path.basename(file)
        file_date = None
        searchisodate = re.search(r'(^|[^0-9])(20[0-9][0-9])-?([01][0-9])-?([0-9][0-9])', filename)
        searchisodate2 = re.search(r'(^|[^0-9])(2[0-9])-?([01][0-9])-?([0-9][0-9])', filename)
        searchfradate = re.search(r'(^|[^0-9])([0-9][0-9])[-_]?([01][0-9])[-_]?(20[0-9][0-9])', filename)
        searchpdfdate = None
        if meta.get('CreationDate') and isinstance(meta['CreationDate'], str):
            searchpdfdate = re.search(r'(^|[^0-9])(20[0-9][0-9])-?([01][0-9])-?([0-9][0-9])', meta['CreationDate'])
        if meta.get('facture:date'):
            file_date = meta['facture:date']
        elif searchisodate and int(searchisodate.group(2)) > 2000 and int(searchisodate.group(2)) < 2100 and int(searchisodate.group(3)) < 13 and int(searchisodate.group(4)) < 32:
            file_date = searchisodate.group(2) + '-' + searchisodate.group(3) + '-' + searchisodate.group(4)
        elif searchisodate2 and int(searchisodate2.group(2)) > 20 and int(searchisodate2.group(2)) < 30 and int(searchisodate2.group(3)) < 13 and int(searchisodate2.group(4)) < 32:
            file_date = '20' + searchisodate2.group(2) + '-' + searchisodate2.group(3) + '-' + searchisodate2.group(4)
        elif searchfradate and int(searchfradate.group(4)) > 2000 and int(searchfradate.group(4)) < 2100 and int(searchfradate.group(3)) < 13 and int(searchfradate.group(2)) < 32:
            file_date = searchfradate.group(4) + '-' + searchfradate.group(3) + '-' + searchfradate.group(2)
        elif searchpdfdate and int(searchpdfdate.group(2)) > 2000 and int(searchpdfdate.group(2)) < 2100 and int(searchpdfdate.group(3)) < 13 and int(searchpdfdate.group(4)) < 32:
            file_date = searchpdfdate.group(2) + '-' + searchpdfdate.group(3) + '-' + searchpdfdate.group(4)
        else:
            file_date = time.strftime('%Y-%m-%d', time.gmtime(meta['ctime']))

        res = conn.execute("SELECT id FROM pdf_file WHERE fullpath = \"%s\" OR md5 = \"%s\"" % (file, meta['md5']))
        has_file = res.fetchone()
        if not has_file:
            sql = "INSERT INTO pdf_file (fullpath, filename, md5, date, ctime, mtime, extention) VALUES (\"%s\", \"%s\", \"%s\", \"%s\", %d, %d, \"image\") ; " % (file, filename, meta['md5'], file_date, meta['ctime'], meta['mtime'])
            conn.execute(sql)
        else:
            sql = 'UPDATE pdf_file SET filename = "%s", md5 = "%s", mtime = %d, date = "%s" WHERE fullpath = "%s" OR md5 = "%s"' % (filename, meta['md5'], meta['mtime'], file_date, file, meta['md5'])
            conn.execute(sql)

    @staticmethod
    def index_pdf(file, last, conn):
        excludes = os.environ.get('COMPTA_PDF_EXCLUDE_PATH', '')
        for exclude in excludes.split('|'):
            if exclude and file.find(exclude) > -1:
                return False

        mtime = os.path.getmtime(file)
        if  mtime <= last:
            return False

        fp = open(file, 'rb')
        parser = PDFParser(fp)
        doc = PDFDocument(parser)
        try:
            meta = doc.info[0]
        except IndexError:
            meta = {}

        try:
            if not meta.get('facture:TTC'):
                meta = meta | Indexer.index_pdfattachment(doc)
        except Exception:
            meta = meta

        meta['file'] = file
        meta = Indexer.homogeneise_meta(meta)

        fp.seek(0)
        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: fp.read(4096), b""):
            hash_md5.update(chunk)
        meta['md5'] = hash_md5.hexdigest()
        meta['ctime'] = os.path.getctime(file)
        meta['mtime'] = mtime
        filename = os.path.basename(file)
        file_date = None
        searchisodate = re.search(r'(^|[^0-9])(20[0-9][0-9])-?([01][0-9])-?([0-9][0-9])', filename)
        searchfradate = re.search(r'(^|[^0-9])([0-9][0-9])[-_]?([01][0-9])[-_]?(20[0-9][0-9])', filename)
        searchpdfdate = None
        if meta.get('CreationDate') and isinstance(meta['CreationDate'], str):
            searchpdfdate = re.search(r'(^|[^0-9])(20[0-9][0-9])-?([01][0-9])-?([0-9][0-9])', meta['CreationDate'])
        if meta.get('facture:date'):
            file_date = meta['facture:date']
        elif searchisodate and int(searchisodate.group(2)) > 2000 and int(searchisodate.group(2)) < 2100 and int(searchisodate.group(3)) < 13 and int(searchisodate.group(4)) < 32:
            file_date = searchisodate.group(2) + '-' + searchisodate.group(3) + '-' + searchisodate.group(4)
        elif searchfradate and int(searchfradate.group(4)) > 2000 and int(searchfradate.group(4)) < 2100 and int(searchfradate.group(3)) < 13 and int(searchfradate.group(2)) < 32:
            file_date = searchfradate.group(4) + '-' + searchfradate.group(3) + '-' + searchfradate.group(2)
        elif searchpdfdate and int(searchpdfdate.group(2)) > 2000 and int(searchpdfdate.group(2)) < 2100 and int(searchpdfdate.group(3)) < 13 and int(searchpdfdate.group(4)) < 32:
            file_date = searchpdfdate.group(2) + '-' + searchpdfdate.group(3) + '-' + searchpdfdate.group(4)
        else:
            file_date = time.strftime('%Y-%m-%d', time.gmtime(meta['ctime']))

        if meta.get('ModDate'):
            searchpdfdate = re.search(r'(^|[^0-9])(20[0-9][0-9])-?([01][0-9])-?([0-9][0-9])', meta['ModDate'])
            if searchpdfdate and int(searchpdfdate.group(2)) > 2000 and int(searchpdfdate.group(2)) < 2100 and int(searchpdfdate.group(3)) < 13 and int(searchpdfdate.group(4)) < 32:
                file_date2 = searchpdfdate.group(2) + '-' + searchpdfdate.group(3) + '-' + searchpdfdate.group(4)
            if file_date2 < file_date:
                file_date = file_date2

        res = conn.execute("SELECT id FROM pdf_file WHERE fullpath = \"%s\" OR md5 = \"%s\"" % (file, meta['md5']))
        has_file = res.fetchone()
        if not has_file:
            sql = "INSERT INTO pdf_file (fullpath, filename, md5, date, ctime, mtime, extention) VALUES (\"%s\", \"%s\", \"%s\", \"%s\", %d, %d, \"pdf\") ; " % (file, filename, meta['md5'], file_date, meta['ctime'], meta['mtime'])
            conn.execute(sql)
        else:
            sql = 'UPDATE pdf_file SET filename = "%s", md5 = "%s", mtime = %d, date = "%s" WHERE fullpath = "%s" OR md5 = "%s"' % (filename, meta['md5'], meta['mtime'], file_date, file, meta['md5'])
            conn.execute(sql)

        sql_update = "UPDATE pdf_piece SET "
        sql_update = sql_update + " filename = \"%s\", extention = \"pdf\" " % filename
        sql_update = sql_update + ', md5 = "%s"' % meta['md5']
        sql_update = sql_update + ', mtime = %d' % meta['mtime']
        sql_update = sql_update + ", fullpath = \"%s\" " % file
        need_update = False
        if meta.get('facture:type'):
            sql_update = sql_update + ", facture_type = \"%s\"" % meta['facture:type'].replace(' ', '')
            need_update = True
        if meta.get('facture:author'):
            sql_update = sql_update + ", facture_author = \"%s\" " % meta['facture:author']
            need_update = True
        if meta.get('facture:client'):
            sql_update = sql_update + ", facture_client = \"%s\" " % meta['facture:client']
            need_update = True
        if meta.get('facture:identifier'):
            sql_update = sql_update + ", facture_identifier = \"%s\" " % meta['facture:identifier']
            need_update = True
        elif meta.get('facture:id'):
            sql_update = sql_update + ", facture_identifier = \"%s\" " % meta['facture:id']
            need_update = True
        if meta.get('facture:date'):
            sql_update = sql_update + ", facture_date = \"%s\" " % meta['facture:date']
            exercice_date = date.fromisoformat(meta['facture:date']) + relativedelta(months=-6)
            sql_update = sql_update + ", compta_exercice = \"%s\" " % exercice_date.year;
            need_update = True
        if meta.get('facture:libelle'):
            sql_update = sql_update + ", facture_libelle = \"%s\" " % meta['facture:libelle'];
            need_update = True
        if meta.get('facture:HT'):
            sql_update = sql_update + ', facture_prix_ht = %s ' % str(meta['facture:HT']).replace(',', '.');
            need_update = True
        if meta.get('facture:TVA'):
            sql_update = sql_update + ', facture_prix_tax = %s ' % str(meta['facture:TVA']).replace(',', '.');
            need_update = True
        if meta.get('facture:TTC'):
            sql_update = sql_update + ', facture_prix_ttc = %s ' % str(meta['facture:TTC']).replace(',', '.');
            need_update = True
        if meta.get('facture:devise'):
            sql_update = sql_update + ", facture_devise = \"%s\" " % meta['facture:devise']
            need_update = True
        if meta.get('paiement:comment'):
            sql_update = sql_update + ", paiement_comment = \"%s\" " % meta['paiement:comment']
            need_update = True
        if meta.get('paiement:proof'):
            sql_update = sql_update + ", paiement_proof = \"%s\" " % meta['paiement:proof']
            need_update = True
        if meta.get('paiement:date'):
            sql_update = sql_update + ", paiement_date = \"%s\" " % meta['paiement:date']
            need_update = True
        if meta.get('compta:export_date'):
            sql_update = sql_update + ", compta_export_date = \"%s\" " % meta['compta:export_date']
            need_update = True
        sql_update = sql_update + " WHERE fullpath = \"%s\" OR md5 = \"%s\"" % (file, meta['md5'])
        sql_update = sql_update + " ; "

        if not need_update:
            return True


        res = conn.execute("SELECT * FROM pdf_piece WHERE fullpath = \"%s\" OR md5 = \"%s\"" % (file, meta['md5']))
        has_piece = res.fetchone()
        sql = "INSERT INTO pdf_piece (fullpath, filename, md5, ctime, mtime) VALUES (\"%s\", \"%s\", \"%s\", %d, %d) ; " % (file, filename, meta['md5'], meta['ctime'], meta['mtime'])
        if not has_piece:
            conn.execute(sql)

        conn.execute(sql_update)
        conn.commit()


        if os.environ.get('VERBOSE', None):
            print("Index: %s" % file)

        return True

    @staticmethod
    def index_pdfattachment(document):
        meta = {}
        for xref in document.xrefs:
            for obj_id in xref.get_objids():
                obj = document.getobj(obj_id)
                if obj and hasattr(obj, 'get_data'):
                        data = obj.get_data()
                        if data and (data.startswith(b'<?xml') or data.startswith(b'<') and (data.endswith(b'>') or data.endswith(b'>\n'))):
                            root = etree.fromstring(data)
                            elements = root.xpath('.//*[local-name() = "GrandTotalAmount"]')
                            meta['facture:TTC'] = elements[0].text
                            elements = root.xpath('.//*[local-name() = "TaxBasisTotalAmount"]')
                            meta['facture:HT'] = elements[0].text
                            elements = root.xpath('.//*[local-name() = "ExchangedDocument"]')
                            meta['facture:identifier'] = elements[0][0].text
                            elements = root.xpath('.//*[local-name() = "SellerTradeParty"]')
                            meta['facture:author'] = elements[0][0].text
                            elements = root.xpath('.//*[local-name() = "BuyerTradeParty"]')
                            meta['facture:client'] = elements[0][0].text
                            elements = root.xpath('.//*[local-name() = "DateTimeString"]')
                            meta['facture:date'] = elements[0].text[:4] + '-' + elements[0].text[4:6] + '-'+ elements[0].text[6:]
        return meta

    @staticmethod
    def index_banque(csv_url, force, conn):
        import csv
        import requests
        from io import StringIO
        import datetime

        imported_at = datetime.datetime.now().timestamp()
        updated_at = imported_at

        last = None
        res = conn.execute("SELECT mtime FROM pdf_banque ORDER BY mtime DESC LIMIT 1;");
        fetch = res.fetchone()
        if fetch:
            last = fetch[0]

        if not force and last and (updated_at - last) < 15 * 60:
            return False

        with requests.get(csv_url, stream=True) as r:
            csv_raw = StringIO(r.text)
            csv_reader = csv.reader(csv_raw, delimiter=",")
            for csv_row in csv_reader:
                if (csv_row[0] == 'date') or (len(csv_row) < 7):
                    continue
                csv_row[1] = re.sub(r'  +', ' ', csv_row[1])
                csv_row[7] = re.sub(r'  +', ' ', csv_row[7])
                sql = "SELECT id FROM pdf_banque WHERE date = \"%s\" AND raw = \"%s\";" % (csv_row[0], csv_row[1])
                res = conn.execute(sql)
                row = res.fetchone()
                if not row or not row[0]:
                    sql = "INSERT INTO pdf_banque (date, raw, ctime) VALUES (\"%s\", \"%s\" , \"%s\")" % (csv_row[0], csv_row[1], imported_at)
                    conn.execute(sql)
                sql = "UPDATE pdf_banque SET "
                sql = sql + 'amount = %s, ' % csv_row[2]
                sql = sql + 'type = "%s", ' % csv_row[3]
                sql = sql + 'banque_account = "%s", ' % csv_row[4]
                sql = sql + 'rdate = "%s", ' % csv_row[5]
                sql = sql + 'vdate = "%s", ' % csv_row[6]
                sql = sql + 'label = "%s", ' % csv_row[7]
                sql = sql + 'mtime = %d' % updated_at
                sql = sql + " WHERE date = \"%s\" AND raw = \"%s\";" % (csv_row[0], csv_row[1])
                conn.execute(sql)

        if last:
            res = conn.execute("UPDATE pdf_banque SET mtime = %d WHERE mtime = %d LIMIT 1" % (updated_at, last));

        conn.commit()


        if os.environ.get('VERBOSE', None):
            print("Indexed: Banque")

        return True

    @staticmethod
    def consolidate(conn):
        res = conn.execute("SELECT id, date, raw, label FROM pdf_banque");
        proof2banqueid = {}


        if os.environ.get('VERBOSE', None):
            print("Index: consolidating")


        for row in res:
            if row['raw']:
                proof2banqueid[re.sub(r'  *', ' ', row['raw']) + 'ø' + row['date']] = row['id'];
            if row['label']:
                proof2banqueid[re.sub(r'  *', ' ', row['label']) + 'ø' +  row['date']] = row['id'];

        md52pid = {}
        res = conn.execute("SELECT id, paiement_proof, paiement_date, fullpath, md5 FROM pdf_piece")
        for row in res:
            md52pid[row['md5']] = row['id']
            banqueid = None
            if not row['paiement_proof']:
                continue
            if not row['paiement_date']:
                continue
            proofs = row['paiement_proof'].split('|')
            dates = row['paiement_date'].split('|')
            if len(proofs) > 1 and len(proofs) != len(dates):
                continue
            for i in range(0,len(dates)):
                paiement_date = dates[i]
                try:
                    paiement_proof = proofs[i]
                except:
                    paiement_proof = proofs[0]
                paiement_proof = re.sub(r'  *', ' ', paiement_proof)
                paiement_proof = re.sub(r'^  *', '', paiement_proof)
                paiement_proof = re.sub(r'  *$', '', paiement_proof)
                if not paiement_proof:
                    continue
                if paiement_date:
                    banqueid = proof2banqueid.get(paiement_proof + 'ø' + paiement_date)
                if not banqueid:
                    ids = []
                    for pkey in proof2banqueid:
                        (label, date) = pkey.split('ø')
                        label = re.sub(r'  *', ' ', label)
                        label = re.sub(r'^  *', '', label)
                        label = re.sub(r'  *$', '', label)
                        if label.find(paiement_proof) != -1 or paiement_proof.find(label) != -1:
                            if date == paiement_date:
                                ids.append(proof2banqueid[pkey])
                    if len(ids) == 1:
                        banqueid = ids[0]
                if banqueid:
                    conn.execute("UPDATE pdf_piece SET banque_id = %d WHERE id = %d" % (banqueid,  row['id']) )
                    conn.execute("UPDATE pdf_banque SET piece_id = %d WHERE id = %d" % (row['id'], banqueid) )
                    conn.execute("UPDATE pdf_file SET date = \"%s\" WHERE date IS NULL AND md5 = \"%s\"" % (paiement_date, row['md5']))
                else:
                    paiement_facture_id = re.sub(r'[^0-9]*', '', paiement_proof)
                    if paiement_facture_id:
                        res = conn.execute("SELECT id FROM pdf_piece WHERE facture_identifier = \"%s\" and facture_date = \"%s\";" % (paiement_facture_id, dates[0]));
                        rows = res.fetchall()
                        if len(rows) == 1:
                            conn.execute("UPDATE pdf_piece SET banque_id = 999000%d WHERE id = %d" % (rows[0]['id'],  row['id']))


        conn.commit()

        res = conn.execute("SELECT id, md5 FROM pdf_file WHERE piece_id IS NULL")
        for row in res:
            if md52pid.get(row['md5']):
                res = conn.execute("UPDATE pdf_file SET piece_id = '%s' WHERE id = %d" % (md52pid[row['md5']], row['id']))
        conn.commit()


    @staticmethod
    def update_path(path, with_images, force):
        with sqlite3.connect('db/database.sqlite') as conn:
            conn.row_factory = sqlite3.Row
            need_consolidate = False
            last = 0
            try:
                res = None
                if with_images:
                    res = conn.execute("SELECT mtime FROM pdf_file WHERE fullpath LIKE \"" + path + "%\" AND filename NOT LIKE \"%pdf\" ORDER BY mtime DESC LIMIT 1;");
                else:
                    res = conn.execute("SELECT mtime FROM pdf_file WHERE fullpath LIKE \"" + path + "%\" ORDER BY mtime DESC LIMIT 1;");
                row = res.fetchone()
                if row:
                    last = row[0]
            except sqlite3.OperationalError:

                if os.environ.get('VERBOSE', None):
                    print("Index: creating database")

                conn.execute("CREATE TABLE pdf_file (id INTEGER PRIMARY KEY, filename TEXT, fullpath TEXT UNIQUE, extention TEXT, size INTEGER, date DATE, ctime INTEGER, mtime INTEGER, md5 TEXT, piece_id INTEGER);");
                conn.execute("CREATE TABLE pdf_piece (id INTEGER PRIMARY KEY, filename TEXT, fullpath TEXT UNIQUE, extention TEXT, size INTEGER, ctime INTEGER, mtime INTEGER, md5 TEXT, facture_type TEXT, facture_author TEXT, facture_client TEXT, facture_identifier TEXT, facture_date DATE, facture_libelle TEXT, facture_prix_ht FLOAT, facture_prix_tax FLOAT, facture_prix_ttc FLOAT, facture_devise TEXT, paiement_comment TEXT, paiement_date DATE, paiement_proof TEXT, banque_id INTEGER,      compta_exercice TEXT, compta_export_date TEXT, CONSTRAINT constraint_name UNIQUE (md5) );");
                conn.execute("CREATE TABLE pdf_banque (id INTEGER PRIMARY KEY, date DATE, raw TEXT, amount FLOAT, type TEXT, banque_account TEXT, rdate DATE, vdate DATE, label TEXT, piece_id INTEGER, ctime INTEGER, mtime INTEGER, CONSTRAINT constraint_name UNIQUE (date, raw) );");


            if os.environ.get('VERBOSE', None):
                 print("Index: indexing files from %s" % path)

            for file in glob.glob(path+'/**/*pdf', recursive=True):
                need_consolidate = Indexer.index_pdf(file, last, conn) or need_consolidate
            if with_images:
                for file in glob.glob(path+'/**/*png', recursive=True):
                    need_consolidate = Indexer.index_image(file, last, conn) or need_consolidate
                for file in glob.glob(path+'/**/*jpg', recursive=True):
                    need_consolidate = Indexer.index_image(file, last, conn) or need_consolidate
                for file in glob.glob(path+'/**/*jpeg', recursive=True):
                    need_consolidate = Indexer.index_image(file, last, conn) or need_consolidate

            if os.environ.get('VERBOSE', None):
                print("Index: indexing banque")

            need_consolidate = Indexer.index_banque('https://raw.githubusercontent.com/24eme/banque/master/data/history.csv', force, conn) or need_consolidate

            if need_consolidate:
                Indexer.consolidate(conn)
                conn.commit()

    @staticmethod
    def update(with_images = False, force = False):
        for subdir in os.environ.get('COMPTA_PDF_COMPTA_SUBDIR').split('|'):
            Indexer.update_path(os.environ.get('COMPTA_PDF_BASE_PATH') + '/' + subdir, with_images, force)

def main():
    Indexer.update_path(sys.argv[1], True, True)

if __name__ == "__main__":
    main()

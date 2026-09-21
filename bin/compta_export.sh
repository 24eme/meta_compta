#!/bin/bash

export_date=$1
cd $(dirname $0)/..

. bin/config.inc

if ! test "$export_date"; then
	export_date=$(date "+%Y-%m-%d")
fi
exercice=$(date -d "$export_date $COMPTA_EXERCICE_DATE_AGO" "+%Y")

curl -s http://$COMPTA_BIND/update > /dev/null

echo "SELECT fullpath FROM pdf_piece WHERE compta_exercice = \""$exercice"\" AND facture_date <= \""$export_date"\" AND compta_export_date IS NULL;" | sqlite3 db/database.sqlite | while read pdf;
do
	cp "$pdf" /tmp/pdf
	echo "InfoBegin" > .meta.tmp
	echo "InfoKey: compta:export_date" >> .meta.tmp
	echo "InfoValue: "$export_date >> .meta.tmp
    echo "InfoBegin" >> .meta.tmp
    echo "InfoKey: compta:exercice" >> .meta.tmp
    echo "InfoValue: "$exercice >> .meta.tmp
	pdftk /tmp/pdf update_info .meta.tmp output /tmp/pdf.new
	if file -i /tmp/pdf.new | grep 'application/pdf' > /dev/null; then
		cp /tmp/pdf.new "$pdf"
	fi
	rm /tmp/pdf /tmp/pdf.new .meta.tmp
done

curl -s http://$COMPTA_BIND/update > /dev/null

echo 'SELECT fullpath FROM pdf_piece WHERE compta_exercice = "'$exercice'" AND compta_export_date = "'$export_date'";' | sqlite3 db/database.sqlite | sed 's|'$COMPTA_PDF_BASE_PATH'||' | sed 's|^/||' > /tmp/export.list
if test -s /tmp/export.list ; then
cd $COMPTA_PDF_BASE_PATH
mkdir -p $COMPTA_EXPORT_DIR"/"$export_date"/.zip/"
echo zip $COMPTA_EXPORT_DIR"/"$export_date"/.zip/"$export_date"_24eme_pieces-comptables.zip" $(cat /tmp/export.list | sed 's/^/"/' | sed 's/$/"/') | bash > /dev/null
cd $COMPTA_EXPORT_DIR"/"$export_date"/" 2> /dev/null
cp /tmp/export.list $COMPTA_EXPORT_DIR"/"$export_date"/.zip/"
unzip $COMPTA_EXPORT_DIR"/"$export_date"/.zip/"$export_date"_24eme_pieces-comptables.zip" > /dev/null
echo "Pièces exportées : "$COMPTA_EXPORT_DIR"/"$export_date" et "$COMPTA_EXPORT_DIR"/"$export_date"/.zip/"$export_date"_24eme_pieces-comptables.zip"
fi

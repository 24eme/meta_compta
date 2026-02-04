from django.shortcuts import redirect, render
from django.http import HttpResponse
from django.template import loader

from pdf.models import Banque
from pdf.models import Piece
from pdf.models import File
import Indexer

import os

def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")

def banque_list(request):
    Indexer.Indexer.update(False, request.GET.get('force'))

    last_update = Banque.objects.order_by('-mtime')[0]
    context = {
        "banques": Banque.objects.order_by('-date'),
        "last_updated_tupple": last_update
    }
    return render(request, "banque_list.html", context)

def piece_list(request):
    Indexer.Indexer.update()
    last_update = File.objects.order_by('-mtime')[0]
    pieces_query = Piece.objects
    client = request.GET.get('client')
    if client:
        pieces_query = pieces_query.filter(facture_client=client)
    author = request.GET.get('author')
    if author:
        pieces_query = pieces_query.filter(facture_author=author)

    unpaid = False
    if request.GET.get('unpaid'):
        pieces_query = pieces_query.filter(banque_id = None)
        unpaid = True

    query_string = '?'
    for arg in request.GET:
        query_string += arg+'='+request.GET.get(arg)+'&'

    if request.GET.get('export') == 'csv':
        return render(request, "export.csv", {'pieces': pieces_query.order_by('-facture_date')}, 'text/csv; charset=utf-8')

    context = {
        "pieces": pieces_query.order_by('-facture_date'),
        "last_updated_tupple": last_update,
        "client": client,
        "author": author,
        "unpaid": unpaid,
        "query_string": query_string
    }
    return render(request, "piece_list.html", context)

def file_update(request):
    Indexer.Indexer.update()
    return HttpResponse("updated")

def file_list(request):
    pdf_only = not request.GET.get('withimages')

    Indexer.Indexer.update(not pdf_only)

    last_update = File.objects.order_by('-date')[0]
    files = File.objects
    if pdf_only:
        files = files.filter(extention="pdf")
    context = {
        "files": files.order_by('-date'),
        "last_updated_tupple": last_update,
        "pdf_only": pdf_only
    }
    return render(request, "file_list.html", context)

def file_check(request):
    files_id = File.objects.values('id')
    for id in files_id:
        try:
             f = File.objects.filter(id=id)
             f.date
        except:
            raise Exception("error with file record "+str(id))
    return HttpResponse("Pas d'erreur trouvé")


def pdf_edit(request, md5):
    files = File.objects.filter(md5=md5).order_by('-mtime')
    file = files[0]
    banque = None
    if request.GET.get('banque_id'):
        banque = Banque.objects.get(pk=request.GET.get('banque_id'))
    elif file and file.piece and file.piece.banque_id and file.piece.banque_id < 999000:
        banque = Banque.objects.get(pk=file.piece.banque_id)
    context = {
        "file": file,
        "pdf_edit_full_url": os.environ.get('COMPTA_PDF_URL')+files[0].fullpath.replace(os.environ.get('COMPTA_PDF_BASE_PATH'), '').replace('+','%2b'),
        "banque": banque,
        "back_banque": request.GET.get('back') == 'banque'
    }
    return render(request, "pdf_edit.html", context)

def compare_strings(a, b):
    if not a:
        return 1
    a = a.upper()
    b = b.upper()
    a_ngrams = list()
    b_ngrams = list()
    for i in range(0, len(a) - 4):
        a_ngrams.append(a[i:i+4])

    if len(a) <= 4:
        a_ngrams.append(a)

    for i in range(0, len(b) - 4):
        b_ngrams.append(b[i:i+4])

    return 1 - len(list(set(a_ngrams) & set(b_ngrams))) / len(a_ngrams);


def piece_pre_associate(request, id):
    Indexer.Indexer.update()
    file = File.objects.filter(id=id).first()
    if not file:
        return redirect('/banque')
    return redirect('/piece/'+file.md5)


def piece_associate_banque(request, md5):
    piece = Piece.objects.filter(md5=md5).first()
    if not piece:
        return redirect('/banque')
    file = piece.getFile()
    banques = {}
    banque_objects = Banque.objects.filter(piece=None)
    if request.GET.get('all'):
        banque_objects = Banque.objects.all()
    for banque in banque_objects:
        distance = 0
        nb = 0
        if piece.facture_author != "24eme" and piece.facture_author != "24ème":
            distance += compare_strings(piece.facture_author, banque.raw)
            nb += 1
        distance += compare_strings(piece.facture_client, banque.raw)
        nb += 1
        distance += compare_strings(piece.facture_libelle, banque.raw)
        nb += 1
        distance += compare_strings(piece.fullpath, banque.raw)
        nb += 1
        distance += compare_strings(piece.filename, banque.raw)
        nb += 1
        thediff = 0
        if piece.facture_date:
            thediff = (int(banque.date.strftime('%s')) - int(piece.facture_date.strftime('%s'))) / (60*60*24*30*6)
        if piece.facture_date and thediff <= 1 and thediff <= -0.03:
            distance += abs(thediff)
            nb += 1
        else:
            distance += 1
            nb += 1
        if piece.facture_prix_ttc and banque.amount:
            if piece.facture_prix_ttc == banque.amount:
                distance += 0
                nb += 4
            else:
                diffprix = abs(abs(piece.facture_prix_ttc) - abs(banque.amount))
                if diffprix < 10:
                    distance += abs(diffprix) / 10
                    nb += 1
                else:
                    distance += 2
                    nb += 2
        if request.GET.get('all') or thediff >= -0.03 :
            banques[banque.id] = {"distance": distance/nb, "banque": banque}

    banques = dict(sorted(banques.items(), key=lambda x: x[1]['distance']))

    return render(request, "piece_associate_banque.html", {"banques": banques, "piece": piece, "file": file})

def banque_associate_file(request, banque_id):
    banque = Banque.objects.get(pk=banque_id)
    pieces = {}
    piece_objects = Piece.objects.filter(banque=None)
    if request.GET.get('all'):
        piece_objects = Piece.objects.all()
    for piece in piece_objects:
        distance = 0
        nb = 0
        distance += compare_strings(piece.facture_author, banque.raw)
        nb += 1
        distance += compare_strings(piece.facture_client, banque.raw)
        nb += 1
        distance += compare_strings(piece.facture_libelle, banque.raw)
        nb += 1
        distance += compare_strings(piece.fullpath, banque.raw)
        nb += 1
        distance += compare_strings(piece.filename, banque.raw)
        nb += 1
        distance += compare_strings(piece.facture_identifier, banque.raw)
        nb += 1
        if piece.facture_date:
            thediff = (int(banque.date.strftime('%s')) - int(piece.facture_date.strftime('%s'))) / (60*60*24*30*6)
            if thediff <= 1 and thediff >= -0.03:
                distance += abs(thediff)
                nb += 1
        if piece.facture_prix_ttc:
            if abs(piece.facture_prix_ttc) == abs(banque.amount):
                nb += 3
                distance = 0
            else:
                distance += 2 * abs(abs(piece.facture_prix_ttc) - abs(banque.amount)) / abs(piece.facture_prix_ttc)
                nb += 2
        file = piece.getFile()
        if file:
            distance += compare_strings(file.fullpath, banque.raw)
            nb += 1
            distance += compare_strings(file.filename, banque.raw)
            nb += 1
            thediff = (file.ctime - int(banque.date.strftime('%s'))) / (60*60*24*30*6)
            if thediff <= 1 and thediff >= -0.03:
                distance += abs(thediff)
                nb += 1
        pieces[piece.md5] = {"distance": distance/nb, "piece": piece, "file": piece.getFile()}

    for file in File.objects.filter(piece_id=None).filter(extention="pdf"):
        distance = 0
        nb = 0
        distance += compare_strings(file.fullpath, banque.raw)
        nb += 1
        distance += compare_strings(file.filename, banque.raw)
        nb += 1
        #Pas de montant
        distance += 1
        nb += 1
        thediff = (int(banque.date.strftime('%s')) - file.ctime) / (60*60*24*30*6)
        if thediff <= 1 and thediff >= 0.03:
            distance += abs(thediff)
            nb += 1
        pieces[file.md5] = {"distance": distance/nb, "file": file}

    pieces = dict(sorted(pieces.items(), key=lambda x: x[1]['distance']))
    return render(request, "banque_associate_file.html", {"pieces": pieces, "banque": banque})

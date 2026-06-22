from django.db import models

class Banque(models.Model):
    date = models.DateField(null=True)
    raw = models.TextField(null=True)
    amount = models.FloatField(null=True)
    type = models.TextField(null=True)
    banque_account = models.TextField(null=True)
    rdate = models.DateField(null=True)
    vdate = models.DateField(null=True)
    label = models.TextField(null=True)
    piece_id = models.IntegerField(null=True)
    ctime = models.IntegerField(null=True)
    mtime = models.IntegerField(null=True)
    piece_category = models.TextField(null=True)

    class Meta:
        unique_together = ('date', 'raw')
    def getPieceMd5(self):
        p = Piece.objects.get(pk=self.piece_id)
        if not p:
            return None
        return p.md5
    def getModified(self, format='%d/%m/%Y %H:%M:%S'):
        import time
        return  time.strftime(format, time.gmtime(self.mtime))

    def isSuccess(self):
        if self.piece_id:
            return True
        if self.piece_category in ("TVA", "SALAIRE"):
            return True
        return False


class Piece(models.Model):
    filename = models.TextField(null=True)
    fullpath = models.TextField(null=True)
    extention = models.TextField(null=True)
    size = models.IntegerField(null=True)
    ctime = models.IntegerField(null=True)
    mtime = models.IntegerField(null=True)
    md5 = models.CharField(max_length=32, null=True, unique=True)
    facture_type = models.TextField(null=True)
    facture_author = models.TextField(null=True)
    facture_client = models.TextField(null=True)
    facture_identifier = models.TextField(null=True)
    facture_date = models.DateField(null=True)
    facture_libelle = models.TextField(null=True)
    facture_prix_ht = models.FloatField(null=True)
    facture_prix_tax = models.FloatField(null=True)
    facture_prix_ttc = models.FloatField(null=True)
    facture_devise = models.TextField(null=True)
    paiement_comment = models.TextField(null=True)
    paiement_date = models.DateField(null=True)
    paiement_proof = models.TextField(null=True)
    banque = models.ForeignKey(Banque, on_delete=models.SET_NULL, null=True)
    compta_exercice = models.TextField(null=True)
    compta_export_date = models.TextField(null=True)
    piece_category = models.TextField(null=True)

    def getFile(self):
        f = File.objects.filter(md5=self.md5).first()
        return f

class File(models.Model):
    filename = models.TextField(null=True)
    fullpath = models.TextField(null=True, unique=True)
    extention = models.TextField(null=True)
    size = models.IntegerField(null=True)
    date = models.DateField(null=True)
    ctime = models.IntegerField(null=True)
    mtime = models.IntegerField(null=True)
    md5 = models.CharField(max_length=32,null=True)
    piece = models.ForeignKey(Piece, on_delete=models.SET_NULL, null=True)
    def getModified(self, format='%d/%m/%Y %H:%M:%S'):
        import time
        return  time.strftime(format, time.gmtime(self.mtime))

#!/usr/bin/env python
#--*--

import xml.etree.cElementTree as ET
import logging
import sys
import traceback
import datetime
import requests
import csv
from pg import DB
import os

logger = logging.getLogger('cfdi_analysis')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('/var/log/cfdi_an/error.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
if not len(logger.handlers):
    logger.addHandler(fh)
    logger.addHandler(ch)


def getDB():
    db = DB(dbname='cfdi_analysis', user='postgres', passwd='dbuser-2018')
    return db

def main():
    try:
        db = getDB()
        path_upload='/home/pgarcia/Documentos/cfdi_analysis/upload/'

        with open(os.path.join(path_upload,'dic18_csv.csv'),'rb') as csvfile:
            fieldnames=['uuid','rfc_emisor','nombre_emisor','rfc_receptor','nombre_receptor','rfc_pac','fecha_emision','fecha_certificacion_sat','monto','tipo_comprobante','estatus','fecha_cancelacion']
            sreader=csv.DictReader(csvfile,fieldnames=fieldnames, delimiter='|', quotechar='|')
            for row in sreader:
                db.insert('sat_meta.dic_18_c',row)

        # with open(os.path.join(path_upload,'Facreviewenero2018-hoy_csv.csv'),'rb') as csvfile:
        #     fieldnames=['folio','tipo_xml','fecha_emision','fecha_de_carga','fecha_validacion','rfc_emisor','emisor','rfc_receptor','receptor','proceso','estatus','fecha_de_cancelacion','subtotal','descuento','iva_r','isr_r','ietu_r','imp_local_r','total_retenciones','iva_t','isr_t','ietu_t','ieps_t','imp_local_t','total_traslados','total','uuid','contabilidad','tipo_doc','cv_rel','id_rel','o_compra','tipo_comprobante','moneda','tipo_cambio','primer_concepto','tua','otros_cargos_aerolineas','metodo_de_pago','errores','anexo','importes','presunto_apocrifos','morosos','sat','version_cfdi','forma_de_pago','folio_relacionado','uuid_relacionado','folio_relacionado_pago','uuid_relacionado_pago','fecha_del_pagado','monto_pago','importe_saldo_anterior','importe_del_pago','importe_saldo_insoluto','moneda_pago','tipo_de_cambio_pago','clave_producto_servicio','es_cancelable']
        #     sreader=csv.DictReader(csvfile,fieldnames=fieldnames,delimiter='|',quotechar='"')
        #     for row in sreader:
        #         # logger.info(row)
        #         try:
        #             db.insert('sat_meta.all_2018',row)
        #         except:
        #             logger.error("error en uuid: %s"%row['uuid'])


        ##rutina para convertir txt con separador ~ a csv, queda con csv separado por comas ','
        # with open ('/home/pgarcia/Documentos/cfdi_analysis/upload/prueba_txt/test_txt.txt', 'r') as f:
        #     stripped=(line.strip() for line in f)
        #     lines=(line.split("~") for line in stripped if line)
        #     with open('/tmp/txt_a_csv.csv', 'w') as of:
        #         writer=csv.writer(of)
        #         writer.writerows(lines)




    except:
        exc_info = sys.exc_info()
        logger.error(traceback.format_exc(exc_info))


if __name__ == '__main__':
    main()

# WhereAreMyLikes

El objetivo de este módulo es saber dónde están nuestros followers. Aquí recibe los likes de kafka y pregunta a la api de twitter a cada usuario de cada like donde ha geoetiquetado sus ultimas 20 fotos y guarda todas las coordenadas en un csv. Las coordenadas guardadas son almacenadas en un objeto de java (coordinatesSent) que se guarda en disco en formato kryo para guardar su estado por si se cae el servicio.

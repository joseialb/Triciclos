from pyspark import SparkContext
import sys
import itertools

def get_vertices(line):
    arista = line.strip().split(',')
    v1 = arista[0]
    v2 = arista[1]
    if v1 == v2:
        return [(v1, v2)]
    else:
        return [(v1, v2), (v2, v1)]

def adyacentes(sc, filename):
    vertices = sc.textFile(filename).\
        map(lambda x: tuple(x.split(','))).\
        groupByKey().\
        mapValues(set).\
        mapValues(sorted).\
        map(lambda x: (x[0], list(filter(lambda y: y > x[0], x[1])))) 
    ady = vertices.collect()
    for nodo in ady:
        print(nodo[0], list(nodo[1]))
    return vertices

def convListTriciclo(listaAsociada):
    triciclo = []
    for nodo in listaAsociada[1]:
        if nodo != 'exists':
            triciclo.append((nodo[1],)+listaAsociada[0])
    return triciclo

def triciclos(sc, filename):
    ady = adyacentes(sc, filename)

    exists = ady.flatMapValues(lambda x: x).\
        map(lambda x: (x, 'exists'))

    pending = ady.flatMapValues(lambda x: itertools.combinations(x,2)).\
                                map(lambda x: (x[1], ('pending', x[0])))

    listaAsociada = exists.union(pending)

    salida = listaAsociada.groupByKey().\
                            mapValues(list).\
                            filter(lambda x: len(x[1])>1).\
                            flatMap(convListTriciclo)
    salida=salida.collect()
    print(salida)
    return salida


        
def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        triciclos(sc, filename)
        
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Faltan argumentos: python3 {sys.argv[0]} <file>")
    else:
        main(sys.argv[1])

module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
belongs :: Eq k => k -> Dict k v -> Bool
belongs = \k dict -> not ( null (filter (\e -> fst e == k) dict) )

(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs 
--Main> [("calle",[3]),("city",[2,1])] ? "city" 
--True

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v
get = \k dict -> snd ((filter (\(x,y) -> x==k) dict) !! 0)

(!) :: Eq k => Dict k v -> k -> v
(!) = flip get
--Main> [("calle",[3]),("city",[2,1])] ! "city" 
--[2,1]

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith = \f k v dict -> if dict ? k then updateDict f k v dict else insertInDict k v dict
										
updateDict :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
updateDict f k v dict = foldr (\e rec -> if fst e == k then (k, (f (snd e) v )) : rec else e:rec ) [] dict 

insertInDict :: Eq k => k -> v -> Dict k v -> Dict k v
insertInDict k v dict = dict ++ [(k,v)] 

--Main> insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
--[(1,"lab"),(2,"p")]

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey = \l -> foldl (\rec (k,v)-> insertWith (++) k [v] rec) [] l

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith = \f d1 d2 -> foldl (\rec (k, v) -> insertWith f k v rec) [] (d1 ++ d2) 

--Main> unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
--[("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]


-- ------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6

distributionProcess :: Int -> [a] -> [[a]]
distributionProcess n l = foldl (\rec e -> (tail rec) ++ [(head rec) ++ [e]] ) (replicate n []) l

------------------------------------

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]
mapperProcess f l = groupByKey ( concat (map f l) )

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]
combinerProcess ls = map (\(k, l) -> (k, concat l)) (sortByKey (groupByKey ( concat ls )))

sortByKey::(Eq k, Ord k) => [(k, [v])] -> [(k,[v])]
sortByKey = sortBy ( (\e1 e2 -> compare (fst e1) (fst e2) ) )

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]
reducerProcess f l = concat ( map f l)

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce mapper reducer l =  reducerProcess reducer (combinerProcess (map (mapperProcess mapper) (distributionProcess 100 l) ) )

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int

-- Otra opcion:
visitasPorMonumento = \lin -> mapReduce mapper reducer lin
 where mapper = \e -> [(e, 1)]
       reducer = \(k, l) -> [(k, (length l))]

-- let visitasPorMonumento l = mapReduce (\e -> [(e, 1)]) (\(k, l) -> [(k, (lenght l))]) l

-- Ejercicio 12
-- Idea-ejemplo:
--	["m1", "m2", "m2"] 
--		visitasPorMonumentos --> [("m1", 1),("m2", 2)]
--			mapper->[(-1, "m1"), (-2, "m2")] 
--				orderByKey --> [(-2, "m2"), (-1, "m1")] 
--					reducecer -> ["m2", "m1"]
monumentosTop:: [String] -> [String]
monumentosTop = \ls -> mapReduce mapper reducer ( visitasPorMonumento ls ) 
	where mapper = \(k, v) -> [(-v, k)]
	      reducer = \(k, l) -> l

-- monumentosTop = \l -> mapReduce (\(k,v) -> [(-v,k)]) (\(k,l) ->l) (visitasPorMonumento l)
  

-- Ejercicio 13 
monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais =  \lin -> mapReduce mapper reducer lin
	where mapper (Monument, dict) = [(dict!"country", 1)]
	      mapper (City, dict) = []
	      mapper (Street, dict) = []
	      mapper _ = error "fst(argumento) no es un Structure"
	      reducer = \(pais, l) -> [(pais, (length l))] 

--monumentosPorPais l = mapReduce (\(s,dict) -> if (isMonument s) then [((dict ! "country"),1)] else [] ) ( \(pais,l) -> [(pais, (length l))] ) l


-- ------------------------ Ejemplo de datos del ejercicio 13 ----------------------
data Structure = Street | City | Monument deriving Show

isMonument:: Structure -> Bool
isMonument Street = False
isMonument City = False
isMonument Monument = True

items :: [(Structure, Dict String String)]
items = [
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Street, [
      ("name","Int. Güiraldes"),
      ("latlong","-34.5454,-58.4386"),
      ("country", "Argentina")]),
    (Monument, [
      ("name", "San Martín"),
      ("country", "Argentina"),
      ("latlong", "-34.6033,-58.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Irak"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")])
    ]


------------------------------------------------
------------------------------------------------

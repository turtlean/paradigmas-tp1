--  Para correr los tests deben cargar en hugs el módulo Tests
--  y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import MapReduce

main :: IO ()
main = hspec $ do

  describe "Utilizando Diccionarios" $ do
    it "puede determinarse si un elemento es una clave o no" $ do
       belongs 3 [(3, "A"), (0, "R"), (7, "G")]    `shouldBe` True
       belongs "k" []                              `shouldBe` False
       [("H", [1]), ("E", [2]), ("Y", [0])] ? "R"  `shouldBe` False
       [("V", [1]), ("O", [2]), ("S", [0])] ? "V"  `shouldBe` True

    it "puede obtenerse el valor de una clave si la misma está definida" $ do
      get 4 [(4,"Tiempo"),(5,"Dinero")] `shouldBe` "Tiempo"
      [(1,"Femur"),(2,"Húmero"),(3,"Cúbito")] ! 2 `shouldBe` "Húmero"	       

    --- insertWith --- 

    it "puede insertarse un elemento (clave,valor) en el diccionario si la clave no está definida" $ do
      insertWith (+) "valor" 3 [("eficiencia",2)] `shouldBe` [("eficiencia",2),("valor",3)]
      insertWith (++) "valor" [3] [("eficiencia",[2])] `shouldBe` [("eficiencia",[2]),("valor",[3])]
      insertWith (++) "valor" [3] [] `shouldBe` [("valor",[3])]

    it "puede insertarse un elemento (clave,valor) en el diccionario cuando la clave ya está definida" $ do
      insertWith (+) "valor" 3 [("valor",5),("eficiencia",3)] `shouldBe` [("valor",8),("eficiencia",3)]
      insertWith (++) "valor" [3] [("eficiencia",[2]), ("valor",[1])] `shouldBe` [("eficiencia",[2]),("valor",[1,3])]

   --- groupByKey

    it "puede agruparse correctamente por clave" $ do
      groupByKey [] `shouldBe` ([]::Dict Int String)
      groupByKey [(1,"one"),(2,"two"),(1,"uno"),(2,"dos")] `shouldBe` [(1,["one","uno"]),(2,["two","dos"])]

   --- unionWith

    it "pueden mergearse correctamente 2 diccionarios" $ do
      unionWith (++) [] [] `shouldBe` ([]::Dict Int [String])
      unionWith (++) [(1,["poemas"]),(2,["fútbol"])] [(1,["ensayos"])] `shouldBe` [(1,["poemas","ensayos"]),(2,["fútbol"])]  

  describe "Mapper & Reducer" $ do
    --- distrbutionProcess               
    it "la carga es distribuída de forma balanceada" $ do
      distributionProcess 5 [1..10] `shouldBe` [[1,6],[2,7],[3,8],[4,9],[5,10]]
       
    --- mapperProcess 
    it "se aplica correctamente la función de mapeo y los resultados son agrupados por clave" $ do
      mapperProcess (\e -> [(e,1)]) [] `shouldBe` ([]::Dict String [Int])
      mapperProcess (\e -> [(e,1)]) ["dolar","euro","peso","real", "euro", "euro"] `shouldBe` [("dolar",[1]),("euro",[1,1,1]),("peso",[1]),("real",[1])]

    --- combinerProcess
    it "se agrupa los resultados por clave y se ordenan de forma creciente" $ do
      combinerProcess [] `shouldBe` ([]::[(Int,[String])])
      combinerProcess [[(3,["hospital","escuela"]),(2,["centro comercial","restaurant"])],[(2,["empresa"]),(1,["Congreso"])]] `shouldBe` [(1,["Congreso"]),(2,["centro comercial","restaurant","empresa"]),(3,["hospital","escuela"])]

    --- reducerProcess 
    it "se aplica correctamente el reducer sobre cada uno de los elementos" $ do
      reducerProcess (\(k,l) -> [(k, (length l))]) []  `shouldBe` ([]::Dict String Int) 
      reducerProcess (\(k,l) -> [(k, (length l))]) [("moluscos",[1,1,1]),("cetáceos",[1,1])]  `shouldBe` [("moluscos",3),("cetáceos",2)]

	it "se obtiene la suma entre la clave y sumatoria de la lista correspondiente" $ do
      reducerProcess (\(k,l) -> [k + (foldl (+) 0 l)]) [(1,[1]),(1,[2]),(2,[1]),(2,[2,3]),(4,[1]),(5,[1,2]),(6,[1]),(3,[1,2,3]),(3,[4]),(2,[4,5]),(3,[5])]  `shouldBe` [2,3,3,7,5,8,7,9,7,11,8]
      	
  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"] `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)] 

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
      `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])

    it "monumentos por pais" $ do
      monumentosPorPais `shouldBe` [("Argentina", 3), ("Francia",1)("Irak",1)]

    it "lista de monumentos" $ do
      mapReduce mapperListaMonus reducerListaMonus items `shouldBe` ["Obelisco","San Martn","Bagdad Bridge"]

    it "se obtiene una lista de tuplas (caracter, cantidad de apariciones)" $ do
      mapReduce mapperCuentaLetras reducerCuentaLetras ["este es un texto de prueba"] `shouldBe` [(' ',5),('a',1),('b',1),('d',1),('e',6),('n',1),('o',1),('p',1),('r',1),('s',2),('t',3),('u',2),('x',1)]

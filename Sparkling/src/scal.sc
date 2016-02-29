object scal {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  def maxi(values: Int*)=values.fold(values(0))(Math.max )
                                                  //> maxi: (values: Int*)Int
  maxi(1,2,3,45,56,33)                            //> res0: Int = 56
  
  
  
  def maxim(values: Int*)=values.foldLeft(0)((a,b) => b)
                                                  //> maxim: (values: Int*)Int
                                                  
                                                  maxim(1,2,3,4,5)
                                                  //> res1: Int = 5
                                                  
   def maximum(values: Int*)=values.foldRight(7)((a,b) => b)
                                                  //> maximum: (values: Int*)Int
                                                  maximum(1,2,2)
                                                  //> res2: Int = 7
                                                  
   def param (name : String ="Naga", last :String="Rayapati")={
   println(s"My name is $name $last")
   name == last
   name eq last
   }                                              //> param: (name: String, last: String)Boolean
   
   param()                                        //> My name is Naga Rayapati
                                                  //| res3: Boolean = false
   param("karthi","ishan")                        //> My name is karthi ishan
                                                  //| res4: Boolean = false
  param(last="Rayapatis")                         //> My name is Naga Rayapatis
                                                  //| res5: Boolean = false
  param(name="Tualsi",last="Rayapati")            //> My name is Tualsi Rayapati
                                                  //| res6: Boolean = false
  param("naga","naga")                            //> My name is naga naga
                                                  //| res7: Boolean = true
  
 
}
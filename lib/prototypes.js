Object.clone = function(obj) {

  if (null == obj || "object" != typeof obj) return obj;

  // Handle using static copy method if available
  if(obj.copy){
    return obj.copy();
  }

  // Handle Object
  if (
    (obj instanceof RegExp) 
    || (obj instanceof Boolean)
    || (obj instanceof Number)
    || (obj instanceof String)
    || (obj.constructor && obj.constructor.prototype === obj)
  ) {
    return obj;
  }
  
  // Handle Date
  if (obj instanceof Date) {
    var copy = new Date();
    copy.setTime(obj.getTime());
    return copy;
  }

  // Handle Array
  if (obj instanceof Array) {
    var copy = [];
    for (var i = 0, len = obj.length; i < len; ++i) {
      copy[i] = Object.clone(obj[i]);
    }
    return copy;
  }

  // Handle Object
  if (obj instanceof Object) {
    var copy = {};
    for (var attr in obj) {
      if (obj.hasOwnProperty(attr)) copy[attr] = Object.clone(obj[attr]);
    }
    return copy;
  }
  
  throw new Error("Unable to copy obj! Its type isn't supported.");
}

Object.shallowCopy = function(object){
  var n = {};
  var keys = Object.keys(object);
  for(var i in keys){
    var key = keys[i];
    n[key] = object[key];
  }
  
  return n;
}

Object.merge = function(object, default_object){
  var new_default_object = Object.clone(default_object);
  Object.update(new_default_object, object)
  return new_default_object;
}

Object.update = function(self, new_object){
  
  for(var key in new_object){
    var v = new_object[key];
    
    if( 
      (v instanceof Object) 
      && (key in self)
      && !(v instanceof Array)
      && !(v instanceof Date) 
      && !(v instanceof RegExp)
      && !(v instanceof Boolean)
      && !(v instanceof Number)
      && !(v instanceof String)
      && !(v.constructor && v.constructor.prototype === v)
    ){
      if(self[key] == null){
        self[key] = {};
      }
      
      Object.update(self[key], v);
      
    }else{
      self[key] = Object.clone(v);
    }
    
  }
}

var Helenus;

module.exports = {
  'setUp':function(test, assert){
    Helenus = require('helenus');
    test.finish();
  },
  
  'test uuid library': function(test, assert){
    var v4 = new Helenus.UUID('e59578a0-bf3f-47b4-bcf0-94f9279271cc'),
        v4buf = new Buffer([0xe5, 0x95, 0x78, 0xa0, 0xbf, 0x3f, 0x47, 0xb4, 0xbc, 0xf0, 0x94, 0xf9, 0x27, 0x92, 0x71, 0xcc]);
        
    assert.ok(v4.hex === 'e59578a0-bf3f-47b4-bcf0-94f9279271cc');
    assert.ok(v4.toBinary() === v4buf.toString('binary'));
    assert.ok(v4.toString() === 'e59578a0-bf3f-47b4-bcf0-94f9279271cc');
    
    var v1 = new Helenus.TimeUUID.fromTimestamp(new Date(1326400762701)),
        v1buf = v1.toBuffer();
    

    assert.ok(v1.hex === (new Helenus.TimeUUID.fromBinary(v1buf.toString('binary')).hex));
    test.finish();
  },
};
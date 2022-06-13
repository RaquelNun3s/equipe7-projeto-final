//Criando o script em java necessário para atender as condições do template "Text to BigQuery" do Dataflow:

function transform(line) {
    var values = line.split(',');
    var obj = new Object();
    obj.ano = values[0];
    obj.material = values[1];
    obj.estado = values[2];
    obj.quantidade = values[3];
    obj.unidade_medida = values[4];
    var jsonString = JSON.stringify(obj);
    return jsonString;
   }
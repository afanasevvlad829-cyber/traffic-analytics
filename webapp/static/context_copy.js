function copyAIContext(code){

fetch("/api/context/"+code)
.then(r=>r.json())
.then(data=>{

let payload=JSON.stringify(data.payload_json,null,2)

let text=`[AI_CONTEXT]
CODE: ${code}

${payload}

QUESTION:
Проанализируй этот объект и дай рекомендации.
[/AI_CONTEXT]`

navigator.clipboard.writeText(text)

alert("Контекст скопирован для AI")

})

}

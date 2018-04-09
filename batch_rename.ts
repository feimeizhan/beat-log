const fs = require('fs'),
    path = require('path'),
    args = process.argv.slice(2),
    dir = args[0],
    match = RegExp(args[1], 'g'),
    replace = args[2];

let files: Array<string> = fs.readdirSync(dir);

files.filter(function (file) {
    return file.match(match);
}).forEach(function (file) {
    let filePath = path.join(dir, file),
        newFilePath = path.join(dir, file.replace(match, replace));

    fs.renameSync(filePath, newFilePath);
});

console.log("done");

// Usage
// node rename.js path/to/directory '\s' '-'

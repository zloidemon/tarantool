/* Alter 'exports' object here */

var formatRegExp = /%[sdj%]/g;
format = function(f) {
    var args = Array.prototype.slice.call(arguments);
    if (typeof(f) !== 'string' || f.indexOf('%') == -1) {
        return args.join(' ');
    }

    var i = 1;
    var len = args.length;
    var str = String(f).replace(formatRegExp, function(x) {
        if (x === '%%') return '%';
        if (i >= len) return x;
        switch (x) {
        case '%s': return String(args[i++]);
        case '%d': return Number(args[i++]);
        case '%j': return JSON.stringify(args[i++]);
        default:
            return x;
        }
    });

    if (i == args.length)
        return str;

    if (str.length > 0) {
        return ' ' + args.slice(i).join(' ')
    } else {
        return args.join(' ')
    }
};

function trace() {
    /* See http://code.google.com/p/v8/wiki/JavaScriptStackTraceApi */
    var prepareStackTraceOld = Error.prepareStackTrace;
    var ret;
    Error.prepareStackTrace = function(error, trace) {
        return trace.slice(1).map(function(frame) {
            frame.typeName = frame.getTypeName();
            frame.function = frame.getFunction();
            frame.functionName = frame.getFunctionName();
            frame.methodName = frame.getMethodName();
            frame.fileName = frame.getFileName();
            frame.lineNumber = frame.getLineNumber();
            frame.columnNumber = frame.getColumnNumber();
            frame.evalOrigin = frame.getEvalOrigin();
            frame.isToplevel = frame.isToplevel();
            frame.isEval = frame.isEval();
            frame.isNative = frame.isNative();
            frame.isConstructor = frame.isConstructor();
            return frame
        });
    }
    var ret = new Error().stack
    Error.prepareStackTrace = prepareStackTraceOld
    return ret;
}

exports.trace = trace

exports.panic = function() {
    var frame = trace()[1]
    return this.say(this.say.FATAL, frame.fileName, frame.lineNumber,
                    format.apply(this, arguments));
}

exports.error = function() {
    var frame = trace()[1]
    return this.say(this.say.ERROR, frame.fileName, frame.lineNumber,
                    format.apply(this, arguments));
}

exports.crit = function() {
    var frame = trace()[1]
    return this.say(this.say.CRIT, frame.fileName, frame.lineNumber,
                    format.apply(this, arguments));

}

exports.warn = function() {
    var frame = trace()[1]
    return this.say(this.say.WARN, frame.fileName, frame.lineNumber,
                    format.apply(this, arguments));

}

exports.info = function() {
    var frame = trace()[1]
    return this.say(this.say.INFO, frame.fileName, frame.lineNumber,
                    format.apply(this, arguments));

}

exports.debug = function() {
    var frame = trace()[1]
    return this.say(this.say.DEBUG, frame.fileName, frame.lineNumber,
                    format.apply(this, arguments));

}

exports.log = exports.info

package logrus_fluent

import (
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/fluent/fluent-logger-golang/fluent"
)

const (
	// TagName is struct field tag name.
	// Some basic option is allowed in the field tag,
	//
	// type myStruct {
	//     Value1: `fluent:"value_1"`    // change field name.
	//     Value2: `fluent:"-"`          // always omit this field.
	//     Value3: `fluent:",omitempty"` // omit this field when zero-value.
	// }
	TagName = "fluent"
	// TagField is logrus field name used as fluentd tag
	TagField = "tag"
	// MessageField is logrus field name used as message.
	// If missing in the log fields, entry.Message is set to this field.
	MessageField = "message"
)

var Prefix = "_"

var AlwaysSentFields logrus.Fields = make(logrus.Fields)

var defaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

// FluentHook is logrus hook for fluentd.
type FluentHook struct {
	// Fluent is actual fluentd logger.
	// If set, this logger is used for logging.
	// otherwise new logger is created every time.
	Fluent     *fluent.Fluent
	PrefixOnly bool

	host   string
	port   int
	levels []logrus.Level
	tag    *string
	app    string

	ignoreFields map[string]struct{}
	filters      map[string]func(interface{}) interface{}
}

// New returns initialized logrus hook for fluentd with persistent fluentd logger.
func New(host string, port int) (*FluentHook, error) {
	return NewAppHook(host, port, "")
}

func NewPrefixHook() *FluentHook {
	return &FluentHook{
		levels:       defaultLevels,
		PrefixOnly:   true,
		tag:          nil,
		ignoreFields: make(map[string]struct{}),
		filters:      make(map[string]func(interface{}) interface{}),
	}
}

// NewAppHook returns initialized logrus hook for fluentd with persistent fluentd logger and sets ther application name.
func NewAppHook(host string, port int, app string) (*FluentHook, error) {
	fd, err := fluent.New(fluent.Config{FluentHost: host, FluentPort: port, MarshalAsJSON: true})
	if err != nil {
		return nil, err
	}

	return &FluentHook{
		levels:       defaultLevels,
		Fluent:       fd,
		PrefixOnly:   false,
		tag:          nil,
		ignoreFields: make(map[string]struct{}),
		filters:      make(map[string]func(interface{}) interface{}),
		app:          app,
	}, nil
}

// NewHook returns initialized logrus hook for fluentd.
// (** deperecated: use New() **)
func NewHook(host string, port int) *FluentHook {
	return &FluentHook{
		host:         host,
		port:         port,
		PrefixOnly:   false,
		levels:       defaultLevels,
		tag:          nil,
		ignoreFields: make(map[string]struct{}),
		filters:      make(map[string]func(interface{}) interface{}),
		app:          "",
	}
}

// Levels returns logging level to fire this hook.
func (hook *FluentHook) Levels() []logrus.Level {
	return hook.levels
}

// SetLevels sets logging level to fire this hook.
func (hook *FluentHook) SetLevels(levels []logrus.Level) {
	hook.levels = levels
}

// Tag returns custom static tag.
func (hook *FluentHook) Tag() string {
	if hook.tag == nil {
		return ""
	}

	return *hook.tag
}

// SetTag sets custom static tag to override tag in the message fields.
func (hook *FluentHook) SetTag(tag string) {
	hook.tag = &tag
}

// AddIgnore adds field name to ignore.
func (hook *FluentHook) AddIgnore(name string) {
	hook.ignoreFields[name] = struct{}{}
}

// AddFilter adds a custom filter function.
func (hook *FluentHook) AddFilter(name string, fn func(interface{}) interface{}) {
	hook.filters[name] = fn
}

// Fire is invoked by logrus and sends log to fluentd logger.
func (hook *FluentHook) Fire(entry *logrus.Entry) error {
	var logger *fluent.Fluent
	var err error

	//if PrefixOnly hook, filter out the prefixes and return
	if hook.PrefixOnly {
		for k := range entry.Data {
			if Prefix != "" && strings.HasPrefix(k, Prefix) {
				delete(entry.Data, k)
			}
		}
		return nil
	}

	switch {
	case hook.Fluent != nil:
		logger = hook.Fluent
	default:
		logger, err = fluent.New(fluent.Config{
			FluentHost:    hook.host,
			FluentPort:    hook.port,
			MarshalAsJSON: true,
		})
		if err != nil {
			return err
		}
		defer logger.Close()
	}

	//add AlwaysSentFields
	for k, v := range AlwaysSentFields {
		entry.Data[k] = v
	}

	if hook.app != "" {
		entry.Data["_app"] = hook.app
	}

	// Create a map for passing to FluentD
	data := make(logrus.Fields)
	for k, v := range entry.Data {
		if _, ok := hook.ignoreFields[k]; ok {
			continue
		}
		if fn, ok := hook.filters[k]; ok {
			v = fn(v)
		}

		//remove the prefix when logging to fluentd and remove fields starting with the prefix for subsequent log Fires
		if Prefix != "" && strings.HasPrefix(k, Prefix) {
			kTrimmed := strings.TrimPrefix(k, Prefix)
			if _, inMap := entry.Data[kTrimmed]; !inMap {
				delete(entry.Data, k)
				k = kTrimmed
			}
		}

		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/Sirupsen/logrus/issues/377
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	setLevelString(entry, data)
	tag := hook.getTag(entry, data)
	if tag != entry.Message {
		setMessage(entry, data)
	}

	fluentData := ConvertToValue(data, TagName)
	err = logger.PostWithTime(tag, entry.Time, fluentData)
	return err
}

// getTagAndDel extracts tag data from log entry and custom log fields.
// 1. if tag is set in the hook, use it.
// 2. if tag is set in custom fields, use it.
// 3. if cannot find tag data, use entry.Message as tag.
func (hook *FluentHook) getTag(entry *logrus.Entry, data logrus.Fields) string {
	// use static tag from
	if hook.tag != nil {
		return *hook.tag
	}

	tagField, ok := data[TagField]
	var tag string
	if ok {
		tag, ok = tagField.(string)
	}

	if !ok {
		if hook.app != "" {
			return hook.app + ".main"
		} else {
			return entry.Message
		}
	}

	if hook.app != "" {
		if tag != "" {
			return hook.app + "." + tag
		} else {
			return hook.app
		}
	} else {
		return tag
	}
}

func setLevelString(entry *logrus.Entry, data logrus.Fields) {
	data["level"] = entry.Level.String()
}

func setMessage(entry *logrus.Entry, data logrus.Fields) {
	if _, ok := data[MessageField]; !ok {
		data[MessageField] = entry.Message
	}
}

package extrastore

type FileUpload struct {
	Attachments []string `json:"Attachments,omitempty"`
	UseCount    int      `json:"UseCount,omitempty"`
}

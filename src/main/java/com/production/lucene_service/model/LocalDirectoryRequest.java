package com.production.lucene_service.model;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class LocalDirectoryRequest {

    @NotBlank(message = "Directory path is required")
    private String directory;
}

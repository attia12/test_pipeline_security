package fr.tictak.dema.controller;

import fr.tictak.dema.exception.ForbiddenException;
import fr.tictak.dema.model.user.User;
import fr.tictak.dema.repository.MoveRequestRepository;
import fr.tictak.dema.repository.ReclamationRepository;
import fr.tictak.dema.repository.UserRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/profile")
@Tag(name = "Gestion du profil", description = "API pour la gestion du profil utilisateur, incluant la mise à jour du mot de passe et du numéro de téléphone.")
public class ProfileController {

    private static final Logger logger = LoggerFactory.getLogger(ProfileController.class);

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final MoveRequestRepository moveRequestRepository;

    private final ReclamationRepository reclamationRepository;

    public ProfileController(UserRepository userRepository, PasswordEncoder passwordEncoder, MoveRequestRepository moveRequestRepository, ReclamationRepository reclamationRepository) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.moveRequestRepository = moveRequestRepository;
        this.reclamationRepository = reclamationRepository;  }

    @Operation(
            summary = "Mettre à jour le mot de passe",
            description = "Met à jour le mot de passe pour l'utilisateur authentifié après vérification de l'ancien mot de passe."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Mot de passe mis à jour avec succès",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = String.class),
                            examples = @ExampleObject(value = """
                                    "Mot de passe mis à jour avec succès"
                                    """))),
            @ApiResponse(responseCode = "400", description = "Requête invalide (champs manquants)",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = String.class),
                            examples = @ExampleObject(value = """
                                    "Champs requis manquants"
                                    """))),
            @ApiResponse(responseCode = "401", description = "Ancien mot de passe incorrect",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = String.class),
                            examples = @ExampleObject(value = """
                                    "Ancien mot de passe incorrect"
                                    """))),
            @ApiResponse(responseCode = "404", description = "Utilisateur non trouvé",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = String.class),
                            examples = @ExampleObject(value = """
                                    "Utilisateur non trouvé avec l'email : utilisateur@example.com"
                                    """))),
            @ApiResponse(responseCode = "401", description = "Authentification invalide ou absente",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = Map.class)))
    })
    @PostMapping("/update-password")
    public ResponseEntity<String> updatePassword(
            @AuthenticationPrincipal String email,
            @RequestBody Map<String, String> request) {
        logger.info("Tentative de mise à jour du mot de passe pour l'email: {}", email);
        String oldPassword = request.get("oldPassword");
        String newPassword = request.get("newPassword");
        if (oldPassword == null || newPassword == null) {
            logger.warn("Champs requis manquants pour la mise à jour du mot de passe pour l'email: {}", email);
            return ResponseEntity.badRequest().body("Champs requis manquants");
        }
        Optional<User> userOptional = userRepository.findByEmail(email);
        if (userOptional.isEmpty()) {
            logUserNotFound(email);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Utilisateur non trouvé avec l'email : " + email);
        }
        User user = userOptional.get();
        if (!passwordEncoder.matches(oldPassword, user.getPassword())) {
            logger.warn("Ancien mot de passe incorrect pour l'email: {}", email);
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Ancien mot de passe incorrect");
        }
        user.setPassword(passwordEncoder.encode(newPassword));
        userRepository.save(user);
        logger.info("Mot de passe mis à jour avec succès pour l'email: {}", email);
        return ResponseEntity.ok("Mot de passe mis à jour avec succès");
    }

    @Operation(
            summary = "Mettre à jour le numéro de téléphone",
            description = "Met à jour le numéro de téléphone pour l'utilisateur authentifié."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Numéro de téléphone mis à jour avec succès",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = String.class),
                            examples = @ExampleObject(value = """
                                    "Numéro de téléphone mis à jour avec succès"
                                    """))),
            @ApiResponse(responseCode = "404", description = "Utilisateur non trouvé",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = String.class),
                            examples = @ExampleObject(value = """
                                    "Utilisateur non trouvé avec l'email : utilisateur@example.com"
                                    """))),
            @ApiResponse(responseCode = "401", description = "Authentification invalide ou absente",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = Map.class)))
    })
    @PostMapping("/update-phone")
    public ResponseEntity<String> updatePhoneNumber(
            @AuthenticationPrincipal String email,
            @RequestBody String newPhoneNumber) {
        logger.info("Tentative de mise à jour du numéro de téléphone pour l'email: {}", email);
        Optional<User> userOptional = userRepository.findByEmail(email);
        if (userOptional.isEmpty()) {
            logUserNotFound(email);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Utilisateur non trouvé avec l'email : " + email);
        }
        User user = userOptional.get();
        user.setPhoneNumber(newPhoneNumber);
        userRepository.save(user);
        logger.info("Numéro de téléphone mis à jour avec succès pour l'email: {}", email);
        return ResponseEntity.ok("Numéro de téléphone mis à jour avec succès");
    }
    @Operation(
            summary = "Supprimer le compte utilisateur",
            description = "Supprime le compte de l'utilisateur authentifié ainsi que toutes les données liées (missions, réclamations, etc.)"
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Compte supprimé avec succès"),
            @ApiResponse(responseCode = "404", description = "Utilisateur non trouvé"),
            @ApiResponse(responseCode = "401", description = "Authentification invalide ou absente")
    })
    @DeleteMapping("/delete-account")
    @Transactional
    public ResponseEntity<String> deleteAccount(@AuthenticationPrincipal String email) {
        logger.info("Demande de suppression de compte pour : {}", email);

        Optional<User> userOptional = userRepository.findByEmail(email);
        if (userOptional.isEmpty()) {
            logUserNotFound(email);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Utilisateur non trouvé avec l'email : " + email);
        }

        User user = userOptional.get();
        String userId = user.getId();

        // Delete related entities BEFORE deleting the user
        try {
            // First delete moves by client ID instead of email if there's a FK constraint
            moveRequestRepository.deleteAllByClientEmail(email);
            logger.debug("MoveRequests supprimés pour userId : {}", email);
        } catch (Exception e) {
            logger.error("Erreur lors de la suppression des MoveRequests pour : {} - {}", email, e.getMessage());
        }

        try {
            // Delete reclamations
            reclamationRepository.deleteAllBySentFromEmail(email);
            logger.debug("Réclamations supprimées pour : {}", email);
        } catch (Exception e) {
            logger.debug("Pas de réclamations à supprimer ou échec pour : {} - {}", email, e.getMessage());
        }



        // Finally delete the user
        try {
            userRepository.deleteById(userId);
            logger.info("Compte et données supprimés pour : {}", email);
            return ResponseEntity.ok("Compte supprimé avec succès");
        } catch (Exception e) {
            logger.error("Erreur lors de la suppression du compte utilisateur : {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Erreur lors de la suppression du compte");
        }
    }


    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<Map<String, String>> handleForbiddenException(ForbiddenException ex) {
        logger.warn("Exception d'accès interdit: {}", ex.getMessage());
        Map<String, String> response = new HashMap<>();
        response.put("error", "Interdit");
        response.put("message", ex.getMessage());
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(response);
    }

    private void logUserNotFound(String email) {
        logger.warn("Utilisateur non trouvé pour l'email: {}", email);
    }
}